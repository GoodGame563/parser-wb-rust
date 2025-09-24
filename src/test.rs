#[cfg(test)]
mod tests {
use crate::structure::{ConcurrentQueue, Record, Root, Product, Color, Size, Price, Data};
    use crate::{read_first_column_structured, get_ids};
    use serde_json;
    use std::sync::{Arc, Mutex};
    use std::io::Cursor;
    use tokio::sync::Semaphore;

  
}