fn main() {
    let count: usize = 0;
    let interval: usize = 16;
    println!("0 is multiple of 16: {}", count.is_multiple_of(interval));

    let count: usize = 16;
    println!("16 is multiple of 16: {}", count.is_multiple_of(interval));

    let count: usize = 15;
    println!("15 is multiple of 16: {}", count.is_multiple_of(interval));
}
