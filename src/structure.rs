use serde::{Deserialize, Serialize};
use std::sync::{Condvar, Mutex};

#[derive(Debug, Deserialize)]
pub struct Record {
    pub first_column: String,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    // name: String,
    // catalog_type: String,
    // catalog_value: String,
    // normquery: String,
    // rmi: String,
    // rs: u32,
    // title: String,
    // search_result: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Color {
    // name: String,
    // id: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Price {
    // basic: u32,
    // product: u32,
    // total: u32,
    // logistics: u32,
    // r#return: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Size {
    // name: String,
    // #[serde(rename = "origName")]
    // orig_name: String,
    // rank: u32,
    // #[serde(rename = "optionId")]
    // option_id: u64,
    // wh: u32,
    // time1: u32,
    // time2: u32,
    // dtype: u32,
    // price: Price,
    // #[serde(rename = "saleConditions")]
    // sale_conditions: u64,
    // payload: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Meta {
    // tokens: Vec<String>,
    // #[serde(rename = "presetId")]
    // preset_id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Product {
    // time1: u32,
    // time2: u32,
    // wh: u32,
    // dtype: u32,
    // dist: u32,
    pub id: u64,
    pub root: u64,
    // #[serde(rename = "kindId")]
    // kind_id: u32,
    // brand: String,
    // #[serde(rename = "brandId")]
    // brand_id: u32,
    // #[serde(rename = "siteBrandId")]
    // site_brand_id: u32,
    // colors: Vec<Color>,
    // #[serde(rename = "subjectId")]
    // subject_id: u32,
    // #[serde(rename = "subjectParentId")]
    // subject_parent_id: u32,
    // name: String,
    // entity: String,
    // #[serde(rename = "matchId")]
    // match_id: u64,
    // supplier: String,
    // #[serde(rename = "supplierId")]
    // supplier_id: u32,
    // #[serde(rename = "supplierRating")]
    // supplier_rating: f32,
    // #[serde(rename = "supplierFlags")]
    // supplier_flags: u32,
    // pics: u32,
    // rating: f32,
    // #[serde(rename = "reviewRating")]
    // review_rating: f32,
    // #[serde(rename = "nmReviewRating")]
    // nm_review_rating: f32,
    // feedbacks: u32,
    // #[serde(rename = "nmFeedbacks")]
    // nm_feedbacks: u32,
    // volume: u32,
    // #[serde(rename = "viewFlags")]
    // view_flags: u32,
    // sizes: Vec<Size>,
    // #[serde(rename = "totalQuantity")]
    // total_quantity: u32,
    // meta: Meta,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Data {
    pub products: Vec<Product>,
    total: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Root {
    // metadata: Metadata,
    // state: u32,
    // version: u32,
    // #[serde(rename = "payloadVersion")]
    // payload_version: u32,
    pub data: Data,
}

struct Queue {
    in_stack: Vec<(Vec<u64>, String)>,
    out_stack: Vec<(Vec<u64>, String)>,
}

impl Queue {
    fn new() -> Self {
        Queue {
            in_stack: Vec::new(),
            out_stack: Vec::new(),
        }
    }

    fn push(&mut self, item: (Vec<u64>, String)) {
        self.in_stack.push(item);
    }

    fn pop(&mut self) -> Option<(Vec<u64>, String)> {
        if self.out_stack.is_empty() {
            while let Some(item) = self.in_stack.pop() {
                self.out_stack.push(item);
            }
        }
        self.out_stack.pop()
    }
}

pub struct ConcurrentQueue {
    queue: Mutex<Queue>,
    condvar: Condvar,
}

impl ConcurrentQueue {
    pub fn new() -> Self {
        ConcurrentQueue {
            queue: Mutex::new(Queue::new()),
            condvar: Condvar::new(),
        }
    }

    pub fn push(&self, item: (Vec<u64>, String)) {
        let mut queue = self.queue.lock().unwrap();
        queue.push(item);
        self.condvar.notify_one();
    }

    pub fn _pop(&self) -> Option<(Vec<u64>, String)> {
        let mut queue = self.queue.lock().unwrap();
        queue.pop()
    }

    pub fn pop_blocking(&self) -> (Vec<u64>, String) {
        let mut queue = self.queue.lock().unwrap();
        loop {
            if let Some(item) = queue.pop() {
                return item;
            }
            queue = self.condvar.wait(queue).unwrap();
        }
    }
}
