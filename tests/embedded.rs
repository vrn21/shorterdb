use shorterdb::ShorterDB;
use std::path::Path;

#[test]
fn test_set_and_get() {
    let mut db = ShorterDB::new(Path::new("./test_db")).unwrap();

    db.set(b"key1", b"value1").unwrap();

    let value = db.get(b"key1").unwrap();
    assert_eq!(value, Some(b"value1".to_vec()));
}

#[test]
fn test_delete() {
    let mut db = ShorterDB::new(Path::new("./test_db")).unwrap();

    db.set(b"key2", b"value2").unwrap();

    db.delete(b"key2").unwrap();

    let value = db.get(b"key2").unwrap();
    assert_eq!(value, None);
}

#[test]
fn test_non_existent_key() {
    let db = ShorterDB::new(Path::new("./test_db")).unwrap();

    // Non-existent keys return Ok(None), not an error
    let value = db.get(b"non_existent_key").unwrap();
    assert_eq!(value, None);
}
