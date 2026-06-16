use std::cmp::Ordering;

pub trait SortedSearch<'a, T: 'a> {
    fn search_by_key<B, F>(&'a self, b: &B, f: F) -> Result<usize, usize>
    where
        F: FnMut(&'a T) -> B,
        B: Ord;
}

impl<'a, T: 'a> SortedSearch<'a, T> for [T] {
    fn search_by_key<B, F>(&'a self, b: &B, mut f: F) -> Result<usize, usize>
    where
        F: FnMut(&'a T) -> B,
        B: Ord,
    {
        if self.is_empty() {
            return Err(0);
        }
        if self.len() < 8 {
            for (i, item) in self.iter().enumerate() {
                let cmp = f(item).cmp(b);
                return match cmp {
                    Ordering::Less => continue,
                    Ordering::Equal => Ok(i),
                    Ordering::Greater => Err(i),
                };
            }
            Err(self.len())
        } else {
            self.binary_search_by_key(b, f)
        }
    }
}
