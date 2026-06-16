use sha2::{Sha256, Digest};
fn main() {
    let bytes = [0, 1, 0, 2, 64, 65]; // Prefix I saw
    println!("{:?}", Sha256::digest(&bytes));
}
