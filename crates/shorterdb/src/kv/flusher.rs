//! Background flusher for ShorterDB.
//!
//! Encapsulates all flush-related state and logic, keeping ShorterDB minimal.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use log::{debug, error, info, warn};

use super::memtable::Memtable;
use super::sst::Sst;
use super::wal::Wal;

/// Manages background flushing of memtables to SST files.
///
/// This struct owns a background thread that performs I/O-heavy flush operations
/// without blocking the main write path.
pub struct Flusher {
    /// Memtable currently being flushed (if any).
    /// Wrapped in Arc for shared access between main thread (reads) and flush thread (writes).
    immutable: Arc<Mutex<Option<Arc<Memtable>>>>,

    /// Condvar for signaling between main and flush thread
    signal: Arc<Condvar>,

    /// Background flush thread handle
    thread: Option<JoinHandle<()>>,

    /// Shutdown flag
    shutdown: Arc<AtomicBool>,
}

impl Flusher {
    /// Create a new flusher with a background thread.
    ///
    /// The flusher takes ownership of SST and WAL via Arc<Mutex<>> for thread-safe access.
    pub fn new(sst: Arc<Mutex<Sst>>, wal: Arc<Mutex<Wal>>) -> Self {
        let immutable = Arc::new(Mutex::new(None));
        let signal = Arc::new(Condvar::new());
        let shutdown = Arc::new(AtomicBool::new(false));

        let thread = {
            let immutable = Arc::clone(&immutable);
            let signal = Arc::clone(&signal);
            let shutdown = Arc::clone(&shutdown);

            thread::spawn(move || {
                debug!("Flush thread started");
                flush_loop(immutable, signal, shutdown, sst, wal);
                debug!("Flush thread exited");
            })
        };

        Self {
            immutable,
            signal,
            thread: Some(thread),
            shutdown,
        }
    }

    /// Schedule a memtable for background flushing.
    ///
    /// Returns immediately unless a previous flush is still in progress,
    /// in which case this method will block (write stall).
    pub fn schedule(&self, memtable: Memtable) {
        let mut guard = self.immutable.lock().unwrap_or_else(|e| e.into_inner());

        // Wait if previous flush not done (write stall)
        if guard.is_some() {
            warn!("Write stall: waiting for previous flush to complete");
        }
        while guard.is_some() && !self.shutdown.load(Ordering::SeqCst) {
            guard = self.signal.wait(guard).unwrap_or_else(|e| e.into_inner());
        }

        // Don't schedule if we're shutting down
        if self.shutdown.load(Ordering::SeqCst) {
            debug!("Flusher shutting down, skipping schedule");
            return;
        }

        debug!("Scheduling memtable for flush");
        *guard = Some(Arc::new(memtable));
        self.signal.notify_all();
    }

    /// Get a reference to the immutable memtable being flushed (for read path).
    ///
    /// Returns None if no flush is in progress.
    pub fn get_immutable(&self) -> Option<Arc<Memtable>> {
        let guard = self.immutable.lock().unwrap_or_else(|e| e.into_inner());
        guard.clone()
    }

    /// Wait for any in-progress flush to complete.
    pub fn wait_for_completion(&self) {
        let mut guard = self.immutable.lock().unwrap_or_else(|e| e.into_inner());

        while guard.is_some() {
            guard = self.signal.wait(guard).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Shutdown the background thread gracefully.
    pub fn shutdown(&mut self) {
        info!("Shutting down flush thread");

        // Signal shutdown
        self.shutdown.store(true, Ordering::SeqCst);
        self.signal.notify_all();

        // Wait for thread to finish
        if let Some(handle) = self.thread.take() {
            // Ignore join errors (thread may have panicked)
            let _ = handle.join();
        }
    }
}

impl Drop for Flusher {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Background thread loop that performs the actual flush operations.
fn flush_loop(
    immutable: Arc<Mutex<Option<Arc<Memtable>>>>,
    signal: Arc<Condvar>,
    shutdown: Arc<AtomicBool>,
    sst: Arc<Mutex<Sst>>,
    wal: Arc<Mutex<Wal>>,
) {
    loop {
        let mem = {
            let mut guard = immutable.lock().unwrap_or_else(|e| e.into_inner());

            // Wait for work or shutdown
            while guard.is_none() && !shutdown.load(Ordering::SeqCst) {
                guard = signal.wait(guard).unwrap_or_else(|e| e.into_inner());
            }

            // Clean exit if shutdown and no pending work
            if shutdown.load(Ordering::SeqCst) && guard.is_none() {
                break;
            }

            // Clone the Arc (cheap, just increments reference count)
            guard.clone()
        };

        if let Some(mem) = mem {
            info!("Starting flush to SST");

            // Perform I/O work (outside lock)
            let flush_result = {
                let mut sst_guard = sst.lock().unwrap_or_else(|e| e.into_inner());
                sst_guard.write_memtable(&mem)
            };

            match flush_result {
                Ok(()) => {
                    info!("Flush to SST completed successfully");

                    // Rotate WAL after successful SST write
                    {
                        let mut wal_guard = wal.lock().unwrap_or_else(|e| e.into_inner());
                        if let Err(e) = wal_guard.rotate() {
                            error!("WAL rotate failed: {}", e);
                            // Continue anyway - SST write succeeded, WAL will be cleared on next flush
                        }
                    }

                    // Clear the immutable slot after successful flush
                    {
                        let mut guard = immutable.lock().unwrap_or_else(|e| e.into_inner());
                        *guard = None;
                    }

                    // Signal completion
                    signal.notify_all();
                }
                Err(e) => {
                    error!("Flush failed: {}", e);
                    // Leave memtable in place for retry
                    // Sleep briefly before retry to avoid spin
                    thread::sleep(std::time::Duration::from_millis(100));
                }
            }
        }
    }
}
