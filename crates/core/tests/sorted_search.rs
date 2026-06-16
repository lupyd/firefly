use firefly_core::sorted_search::SortedSearch;

#[test]
fn test_search_small() {
    let arr = [1, 3, 5, 7];
    assert_eq!(arr.search_by_key(&3, |x| *x), Ok(1));
    assert_eq!(arr.search_by_key(&4, |x| *x), Err(2));
    assert_eq!(arr.search_by_key(&0, |x| *x), Err(0));
    assert_eq!(arr.search_by_key(&8, |x| *x), Err(4));
}

#[test]
fn test_search_large() {
    let arr: Vec<_> = (0..20).collect();
    assert_eq!(arr[..].search_by_key(&15, |x| *x), Ok(15));
    assert_eq!(arr[..].search_by_key(&21, |x| *x), Err(20));
    assert_eq!(arr[..].search_by_key(&-1, |x| *x), Err(0));
}

#[test]
fn test_empty() {
    let arr: [i32; 0] = [];
    assert_eq!(arr.search_by_key(&1, |x| *x), Err(0));
}
