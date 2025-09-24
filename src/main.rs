mod structure;
mod test;

use csv::ReaderBuilder;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use futures::{future::join_all, stream, StreamExt};
use fxhash::FxHashSet;
use reqwest;
use std::{
    error::Error,
    fs::File,
    sync::{
        atomic::{AtomicUsize, Ordering},
        {Arc, Mutex},
    },
    time::{Duration, Instant},
};
use tokio_postgres::NoTls;

use crate::structure::{ConcurrentQueue, Record, Root};

const PAGE_LIMIT: u16 = 50;
const MAX_RETRIES: u8 = 5;
const RETRY_DELAY: Duration = Duration::from_millis(500);
const CHUNK_SISE: usize = 80;

async fn create_pool() -> Result<Pool, Box<dyn Error>> {
    log::info!("Creating database pool...");
    let mut pg_config = tokio_postgres::Config::new();
    pg_config.host("localhost");
    pg_config.port(5433);
    pg_config.user("admin");
    pg_config.password("password");
    pg_config.dbname("internal_db");

    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };
    let mgr = Manager::from_config(pg_config, NoTls, mgr_config);
    let pool = Pool::builder(mgr).max_size(16).build().map_err(|e| {
        log::error!("Failed to create pool: {}", e);
        e
    })?;

    log::info!("Database pool created successfully");
    Ok(pool)
}

async fn fetch_url(word: &str, page: u16) -> Result<String, reqwest::Error> {
    log::debug!("Fetching URL for word: '{}', page: {}", word, page);
    let start_time = Instant::now();
    let url = format!("https://search.wb.ru/exactmatch/ru/common/v9/search?ab_daily_autotest=test_group2&appType=1&curr=rub&dest=-2133466&lang=ru&resultset=catalog&sort=popular&spp=30&suppressSpellcheck=false&query={}&page={}", word, page);

    let result = reqwest::get(&url).await?.text().await;

    log::debug!(
        "Request completed for {} (page {}) in {}ms",
        word,
        page,
        start_time.elapsed().as_millis()
    );
    result
}

async fn get_ids(word: &str, page: u16) -> Vec<u64> {
    let mut ids: Vec<u64> = Vec::new();
    let mut retry_count = 0;

    loop {
        match fetch_url(word, page).await {
            Ok(body) => {
                match serde_json::from_str::<Root>(&body) {
                    Ok(root) => {
                        let count = root.data.products.len();
                        ids.extend(root.data.products.into_iter().map(|p| p.id));
                        log::debug!(
                            "Successfully parsed {} products for '{}' page {}",
                            count,
                            word,
                            page
                        );
                        break;
                    }
                    Err(e) => {
                        log::warn!("JSON parsing error for '{}' page {}: {}\nResponse body: {}", word, page, e, body);
                        break;
                    }
                }
            }
            Err(e) if retry_count < MAX_RETRIES => {
                retry_count += 1;
                log::warn!(
                    "Connection error (attempt {}/{}): {}. Retrying...",
                    retry_count,
                    MAX_RETRIES,
                    e
                );
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(e) => {
                log::error!(
                    "Failed to fetch '{}' page {} after {} attempts: {}",
                    word,
                    page,
                    MAX_RETRIES,
                    e
                );
                break;
            }
        }
    }

    ids
}

async fn fetch_all_products(
    word: &str,
) -> Result<(Vec<u64>, &str), Box<dyn std::error::Error + Send + Sync>> {
    let start_time = Instant::now();
    let semaphore = Arc::new(tokio::sync::Semaphore::new(10));
    let mut tasks = Vec::new();

    for i in 1..PAGE_LIMIT {
        let word = word.to_string();
        let semaphore = Arc::clone(&semaphore);
        tasks.push(tokio::spawn(async move {
            let permit = semaphore.acquire_owned().await.unwrap();
            let result = get_ids(&word, i).await;
            drop(permit);
            result
        }));
    }

    let results = join_all(tasks).await;

    let mut ids = Vec::new();
    for result in results {
        match result {
            Ok(id) => {
                ids.extend(id);
            }
            Err(e) => log::error!("Task panicked: {}", e),
        }
    }

    let elapsed = start_time.elapsed();
    log::info!(
        "Completed collection for '{}' in {:.2}s. Total IDs: {}",
        word,
        elapsed.as_secs_f32(),
        ids.len()
    );

    Ok((ids, word))
}

async fn go_on_all_words(
    start_index: usize,
    queue: Arc<ConcurrentQueue>,
) -> Result<(), Box<dyn Error>> {
    log::info!("Starting processing words from index {}", start_index);
    let data = read_first_column_structured("requests.csv")?;
    let total_items = data.len();
    log::info!("Total words to process: {}", total_items);

    let counter = Arc::new(AtomicUsize::new(start_index));
    let processed = Arc::new(AtomicUsize::new(0));

    let fetcher = stream::iter(std::iter::from_fn(|| {
        let current = counter.fetch_add(1, Ordering::SeqCst);
        if current < total_items {
            Some(fetch_all_products(&data[current]))
        } else {
            None
        }
    }));

    fetcher
        .buffer_unordered(CHUNK_SISE)
        .for_each(|result| async {
            let processed = Arc::clone(&processed);
            {
                match result {
                    Ok((ids, word)) => {
                        let count = processed.fetch_add(1, Ordering::SeqCst) + 1;
                        queue.push((ids, word.to_string()));
                        log::debug!("Processed word '{}' ({} of {})", word, count, total_items);

                        if count % 100 == 0 {
                            log::info!("Progress: {}/{} words processed", count, total_items);
                        }
                    }
                    Err(e) => log::error!("Error processing word: {}", e),
                }
            }
        })
        .await;

    log::info!("Completed all word processing");
    Ok(())
}

async fn create_hash_set(pool: Pool) -> Mutex<FxHashSet<(u64, u64)>> {
    let client = match pool.get().await {
        Ok(client) => client,
        Err(e) => {
            log::error!("Failed to get database connection: {}", e);
            return Mutex::new(FxHashSet::default());
        }
    };
    let rows = client
        .query("SELECT keyword_id, product_id FROM keyword_product", &[])
        .await
        .unwrap();
    let mut pairs = FxHashSet::default();
    for row in rows {
        let keyword_id: i64 = row.get(0);
        let product_id: i64 = row.get(1);
        pairs.insert((keyword_id as u64, product_id as u64));
    }
    drop(client);
    Mutex::new(pairs)
}

async fn check_and_send_to_db(
    queue: Arc<ConcurrentQueue>,
    pool: Pool,
    pairs: Arc<Mutex<FxHashSet<(u64, u64)>>>,
) {
    log::info!("Starting database writer");
    let mut send_data: Vec<(u64, u64)> = Vec::new();
    let mut total_inserted = 0;

    log::debug!("Loading existing pairs from database");
    let client = match pool.get().await {
        Ok(client) => client,
        Err(e) => {
            log::error!("Failed to get database connection: {}", e);
            return;
        }
    };
    drop(client);

    log::info!("Starting main processing loop");

    loop {
        let (data, identifier) = queue.pop_blocking();
        log::debug!("Received {} products for '{}'", data.len(), identifier);

        let insert_start = Instant::now();
        let client = match pool.get().await {
            Ok(client) => client,
            Err(e) => {
                log::error!("Failed to get database connection: {}", e);
                continue;
            }
        };
        let row = match client
            .query_one(
                "INSERT INTO keywords (keyword_text) 
             VALUES ($1) 
             ON CONFLICT (keyword_text) 
             DO UPDATE SET keyword_text = EXCLUDED.keyword_text 
             RETURNING keyword_id",
                &[&identifier],
            )
            .await
        {
            Ok(r) => {
                log::info!(
                    "Inserted/updated keyword '{}' in {}ms",
                    identifier,
                    insert_start.elapsed().as_millis()
                );
                r
            }
            Err(e) => {
                log::error!("Database error for '{}': {}", identifier, e);
                continue;
            }
        };

        let index: Option<i64> = row.try_get(0).unwrap_or(None);
        let index = match index {
            Some(i) => i as u64,
            None => {
                log::error!("Failed to get keyword_id for '{}'", identifier);
                continue;
            }
        };
        let mut pairs = pairs.lock().unwrap();
        let new_items = data
            .iter()
            .filter(|&element| pairs.insert((index, *element)))
            .count();

        log::debug!("Found {} new items for '{}'", new_items, identifier);
        send_data.extend(data.into_iter().map(|element| (index, element)));

        if send_data.len() >= 20000 {
            log::info!("Preparing batch insert of {} items", send_data.len());
            let batch_start = Instant::now();

            let (keyword_ids, product_ids): (Vec<i64>, Vec<i64>) = send_data
                .iter()
                .map(|(k, p)| (*k as i64, *p as i64))
                .unzip();

            let client = match pool.get().await {
                Ok(client) => client,
                Err(e) => {
                    log::error!("Failed to get database connection: {}", e);
                    continue;
                }
            };
            match client
                .execute(
                    "INSERT INTO public.keyword_product (keyword_id, product_id)
                SELECT * FROM UNNEST($1::bigint[], $2::bigint[])
                ON CONFLICT (keyword_id, product_id) DO NOTHING",
                    &[&keyword_ids, &product_ids],
                )
                .await
            {
                Ok(rows) => {
                    total_inserted += rows as usize;
                    let duration = batch_start.elapsed();
                    log::info!(
                        "Batch insert completed: {} rows inserted in {:.2}s (Total: {})",
                        rows,
                        duration.as_secs_f32(),
                        total_inserted
                    );
                    send_data.clear();
                }
                Err(e) => {
                    log::error!("Batch insert failed: {}", e);
                }
            }
        }
    }
}

fn read_first_column_structured(path: &str) -> Result<Vec<String>, Box<dyn Error>> {
    log::info!("Reading CSV file: {}", path);
    let file = File::open(path).map_err(|e| {
        log::error!("Failed to open file {}: {}", path, e);
        e
    })?;

    let mut reader = ReaderBuilder::new().has_headers(false).from_reader(file);
    let mut result = Vec::new();
    let mut line_count = 0;

    for record in reader.deserialize() {
        let record: Record = record.map_err(|e| {
            log::warn!("CSV parsing error at line {}: {}", line_count + 1, e);
            e
        })?;
        result.push(record.first_column);
        line_count += 1;
    }

    log::info!("Read {} lines from CSV file {}", line_count, path);
    Ok(result)
}

fn setup_logger() -> Result<(), fern::InitError> {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{} {}] {}",
                record.level(),
                chrono::Local::now().format("%H:%M:%S"),
                message
            ))
        })
        .level(log::LevelFilter::Info)
        .chain(std::io::stdout())
        .chain(fern::log_file("app.log")?)
        .apply()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    setup_logger().unwrap();
    log::info!("Start app");
    let queue = Arc::new(ConcurrentQueue::new());
    let pool = create_pool().await?;

    let client = pool.get().await?;

    let rows = client
        .query("SELECT max(keyword_id) FROM public.keywords;", &[])
        .await?;
    let index: Option<i64> = rows[0].try_get("max")?;
    let index: usize = match index {
        Some(o) => o as usize,
        None => 0,
    };
    let producer_queue = Arc::clone(&queue);

    let producer = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            match go_on_all_words(index as usize, producer_queue).await {
                Ok(_) => log::info!("End parsing"),
                Err(e) => println!("producer error: {:?}", e),
            }
        });
    });
    println!("rofl");
    let pairs = Arc::new(create_hash_set(pool.clone()).await);
    println!("end read");

    let consumer_pairs_1 = Arc::clone(&pairs);
    let consumer_queue_1 = Arc::clone(&queue);
    let consumer_pool_1 = pool.clone();
    let consumer_1 = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            check_and_send_to_db(consumer_queue_1, consumer_pool_1, consumer_pairs_1).await;
        });
    });

    let consumer_pairs_2 = Arc::clone(&pairs);
    let consumer_queue_2 = Arc::clone(&queue);
    let consumer_pool_2 = pool.clone();
    let consumer_2 = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            check_and_send_to_db(consumer_queue_2, consumer_pool_2, consumer_pairs_2).await;
        });
    });

    let consumer_pairs_3 = Arc::clone(&pairs);
    let consumer_queue_3 = Arc::clone(&queue);
    let consumer_pool_3 = pool.clone();
    let consumer_3 = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            check_and_send_to_db(consumer_queue_3, consumer_pool_3, consumer_pairs_3).await;
        });
    });

    producer.join().unwrap();
    consumer_1.join().unwrap();
    consumer_2.join().unwrap();
    consumer_3.join().unwrap();
    Ok(())
}
