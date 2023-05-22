use herm::request::Fetch;

fn main() {
    println!(
        "Fetch request - {:?}",
        Fetch::new("test".to_string(), 0, 0, 1024).unwrap(),
    );
}
