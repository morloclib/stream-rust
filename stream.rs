// Rust implementations of the stream module's native functions.
//
// A producer receives its sink as a morloc function value; a Rust closure
// becomes one through rustmorloc's blanket impl.

pub type StreamSink<A> = rustmorloc::FnValue1<Vec<A>, ()>;

pub fn stream_fold_source<A: 'static, B: Clone + Send + 'static, F, P, R>(step: F, init: &B, produce: P) -> B
where
    F: Fn(&B, &Vec<A>) -> B + Send + Sync + 'static,
    P: Fn(&StreamSink<A>) -> R,
{
    let acc = std::sync::Arc::new(std::sync::Mutex::new(init.clone()));
    let inner = acc.clone();
    let sink: StreamSink<A> = std::sync::Arc::new(move |xs: &Vec<A>| {
        let mut a = inner.lock().unwrap_or_else(|p| p.into_inner());
        *a = step(&a, xs);
    });
    produce(&sink);
    let out = acc.lock().unwrap_or_else(|p| p.into_inner()).clone();
    out
}

pub fn stream_collect_source<A: Clone + Send + 'static, P, R>(produce: P) -> Vec<A>
where
    P: Fn(&StreamSink<A>) -> R,
{
    let all = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let inner = all.clone();
    let sink: StreamSink<A> = std::sync::Arc::new(move |xs: &Vec<A>| {
        inner.lock().unwrap_or_else(|p| p.into_inner()).extend_from_slice(xs)
    });
    produce(&sink);
    let out = std::mem::take(&mut *all.lock().unwrap_or_else(|p| p.into_inner()));
    out
}

// Summaries merged in stream order as a binary counter: two adjacent
// results of equal rank merge into one of the next rank. Each summary takes
// part in O(log n) merges rather than in every later one, so a summary that
// grows with its input (a table of counts) merges in O(n log n), not O(n^2).
// Only associativity is used.
struct StreamMerges<B> {
    stack: Vec<(u32, B)>,
}

impl<B> StreamMerges<B> {
    fn new() -> Self {
        StreamMerges { stack: Vec::new() }
    }

    fn push<G: Fn(&B, &B) -> B>(&mut self, merge: &G, b: B) {
        let mut v = (0u32, b);
        while self.stack.last().map(|t| t.0) == Some(v.0) {
            let (r, top) = self.stack.pop().unwrap();
            v = (r + 1, merge(&top, &v.1));
        }
        self.stack.push(v);
    }

    fn finish<G: Fn(&B, &B) -> B>(self, merge: &G, empty: B) -> B {
        let mut it = self.stack.into_iter();
        match it.next() {
            None => empty,
            Some((_, first)) => it.fold(first, |acc, (_, b)| merge(&acc, &b)),
        }
    }
}

pub fn stream_reduce_native<A, B, F, G, P>(summarize: F, merge: G, pull: P) -> (bool, String, B)
where
    F: Fn(&Vec<A>) -> B,
    G: Fn(&B, &B) -> B,
    P: Fn() -> (bool, String, Vec<A>),
{
    let mut acc = StreamMerges::new();
    loop {
        let (ok, msg, xs) = pull();
        if !ok || xs.is_empty() {
            return (ok, if ok { String::new() } else { msg }, acc.finish(&merge, summarize(&Vec::new())));
        }
        acc.push(&merge, summarize(&xs));
    }
}

pub fn stream_take_pull_native<A, P>(n: i64, pull: P) -> (bool, String, Vec<A>)
where
    P: Fn() -> (bool, String, Vec<A>),
{
    let n = n.max(0) as usize;
    let mut out = Vec::new();
    while out.len() < n {
        let (ok, msg, xs) = pull();
        if !ok {
            return (false, msg, out);
        }
        if xs.is_empty() {
            break;
        }
        let k = (n - out.len()).min(xs.len());
        out.extend(xs.into_iter().take(k));
    }
    (true, String::new(), out)
}

// External sorting

struct StreamRun {
    file: std::io::BufReader<std::fs::File>,
}

struct StreamSorter {
    dir: String,
    width: usize,
    budget: usize,
    buf: Vec<u64>,
    runs: Vec<std::fs::File>,
    reading: Option<StreamSortedReader>,
}

enum StreamSortedReader {
    Memory(Vec<u64>, usize),
    Merge(Vec<StreamRun>, std::collections::BinaryHeap<std::cmp::Reverse<([u64; 4], usize)>>),
}

fn stream_sorters() -> &'static std::sync::Mutex<std::collections::HashMap<i64, StreamSorter>> {
    static S: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<i64, StreamSorter>>> = std::sync::OnceLock::new();
    S.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

const STREAM_SORTED_BATCH: usize = 1 << 16;
const STREAM_MAX_RUNS: usize = 64;

fn stream_sort_records(xs: &mut [u64], width: usize) {
    fn by<const W: usize>(xs: &mut [u64]) {
        // SAFETY: [u64; W] has the size W * 8 and the alignment of u64, and
        // xs.len() is a multiple of W.
        let rs = unsafe { std::slice::from_raw_parts_mut(xs.as_mut_ptr() as *mut [u64; W], xs.len() / W) };
        rs.sort_unstable();
    }
    match width {
        1 => xs.sort_unstable(),
        2 => by::<2>(xs),
        3 => by::<3>(xs),
        _ => by::<4>(xs),
    }
}

impl StreamSorter {
    fn spill(&mut self) -> Result<(), String> {
        use std::io::Write;
        if self.buf.is_empty() {
            return Ok(());
        }
        stream_sort_records(&mut self.buf, self.width);
        let mut f = self.new_run()?;
        let mut bytes = Vec::with_capacity(1 << 16);
        for xs in self.buf.chunks(1 << 13) {
            bytes.clear();
            for x in xs {
                bytes.extend_from_slice(&x.to_le_bytes());
            }
            f.write_all(&bytes).map_err(|e| format!("cannot write a sort run in {}: {}", self.dir, e))?;
        }
        self.runs.push(f);
        self.buf.clear();
        if self.runs.len() >= STREAM_MAX_RUNS {
            self.compact()?;
        }
        Ok(())
    }

    // An empty file in `dir`, already unlinked.
    fn new_run(&self) -> Result<std::fs::File, String> {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = format!("{}/.sorter-{}-{}", self.dir, std::process::id(), n);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|e| format!("cannot create a sort run in {}: {}", self.dir, e))?;
        std::fs::remove_file(&path).map_err(|e| format!("cannot unlink {}: {}", path, e))?;
        Ok(file)
    }

    // Merge every run into one, so that at most STREAM_MAX_RUNS files are open.
    fn compact(&mut self) -> Result<(), String> {
        use std::io::Write;
        let files = std::mem::take(&mut self.runs);
        let (mut runs, mut heap) = self.merger(files)?;
        let mut w = std::io::BufWriter::with_capacity(1 << 20, self.new_run()?);
        while let Some(mut top) = heap.peek_mut() {
            let std::cmp::Reverse((r, i)) = *top;
            for x in &r[..self.width] {
                w.write_all(&x.to_le_bytes()).map_err(|e| format!("cannot write a sort run in {}: {}", self.dir, e))?;
            }
            match Self::read_record(&mut runs[i], self.width)? {
                Some(n) => *top = std::cmp::Reverse((n, i)),
                None => {
                    std::collections::binary_heap::PeekMut::pop(top);
                }
            }
        }
        let file = w.into_inner().map_err(|e| format!("cannot write a sort run in {}: {}", self.dir, e.error()))?;
        self.runs.push(file);
        Ok(())
    }

    fn merger(&self, files: Vec<std::fs::File>) -> Result<(Vec<StreamRun>, std::collections::BinaryHeap<std::cmp::Reverse<([u64; 4], usize)>>), String> {
        use std::io::Seek;
        let mut runs = Vec::new();
        let mut heap = std::collections::BinaryHeap::new();
        // the runs' read buffers share the memory budget, within bounds
        let cap = (8 * self.budget / files.len().max(1)).clamp(1 << 16, 1 << 20);
        for mut f in files {
            f.seek(std::io::SeekFrom::Start(0)).map_err(|e| format!("cannot read a sort run: {}", e))?;
            let mut run = StreamRun { file: std::io::BufReader::with_capacity(cap, f) };
            if let Some(r) = Self::read_record(&mut run, self.width)? {
                heap.push(std::cmp::Reverse((r, runs.len())));
            }
            runs.push(run);
        }
        Ok((runs, heap))
    }

    fn push(&mut self, xs: &[u64]) -> Result<(), String> {
        if self.reading.is_some() {
            return Err("pushSorter: the sorter is already being read".to_string());
        }
        if xs.len() % self.width != 0 {
            return Err(format!("pushSorter: {} words are not whole records of {}", xs.len(), self.width));
        }
        self.buf.extend_from_slice(xs);
        if self.buf.len() >= self.budget {
            self.spill()?;
        }
        Ok(())
    }

    fn read_record(run: &mut StreamRun, width: usize) -> Result<Option<[u64; 4]>, String> {
        use std::io::{BufRead, Read};
        if run.file.fill_buf().map_err(|e| format!("cannot read a sort run: {}", e))?.is_empty() {
            return Ok(None);
        }
        let mut b = [0u8; 32];
        run.file.read_exact(&mut b[..8 * width]).map_err(|e| format!("cannot read a sort run: {}", e))?;
        let mut r = [0u64; 4];
        for (x, w) in r.iter_mut().zip(b.chunks_exact(8)).take(width) {
            *x = u64::from_le_bytes(w.try_into().unwrap());
        }
        Ok(Some(r))
    }

    fn start(&mut self) -> Result<(), String> {
        if self.runs.is_empty() {
            let mut buf = std::mem::take(&mut self.buf);
            stream_sort_records(&mut buf, self.width);
            self.reading = Some(StreamSortedReader::Memory(buf, 0));
            return Ok(());
        }
        self.spill()?;
        let files = std::mem::take(&mut self.runs);
        let (runs, heap) = self.merger(files)?;
        self.reading = Some(StreamSortedReader::Merge(runs, heap));
        Ok(())
    }

    fn next(&mut self) -> Result<Vec<u64>, String> {
        if self.reading.is_none() {
            self.start()?;
        }
        let width = self.width;
        let mut out = Vec::with_capacity(STREAM_SORTED_BATCH * width);
        match self.reading.as_mut().unwrap() {
            StreamSortedReader::Memory(buf, at) => {
                let k = (STREAM_SORTED_BATCH * width).min(buf.len() - *at);
                out.extend_from_slice(&buf[*at..*at + k]);
                *at += k;
            }
            StreamSortedReader::Merge(runs, heap) => {
                while out.len() < STREAM_SORTED_BATCH * width {
                    let Some(mut top) = heap.peek_mut() else { break };
                    let std::cmp::Reverse((r, i)) = *top;
                    out.extend_from_slice(&r[..width]);
                    match Self::read_record(&mut runs[i], width)? {
                        Some(n) => *top = std::cmp::Reverse((n, i)),
                        None => {
                            std::collections::binary_heap::PeekMut::pop(top);
                        }
                    }
                }
            }
        }
        Ok(out)
    }
}

pub fn stream_open_sorter(dir: &String, width: i64, kib: i64) -> i64 {
    if !(1..=4).contains(&width) {
        panic!("openSorter: record width {} is not 1 to 4 words", width);
    }
    if kib < 1 {
        panic!("openSorter: a memory budget of {} KiB is less than 1", kib);
    }
    if !std::path::Path::new(dir).is_dir() {
        panic!("openSorter: {} is not a directory", dir);
    }
    let width = width as usize;
    let words = (kib as usize) * 1024 / 8;
    let budget = (words - words % width).max(width);
    static NEXT: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(1);
    let h = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let s = StreamSorter { dir: dir.clone(), width, budget, buf: Vec::new(), runs: Vec::new(), reading: None };
    stream_sorters().lock().unwrap_or_else(|p| p.into_inner()).insert(h, s);
    h
}

fn stream_sorter_push(h: i64, xs: &[u64]) -> Result<(), String> {
    let mut table = stream_sorters().lock().unwrap_or_else(|p| p.into_inner());
    match table.get_mut(&h) {
        None => Err(format!("pushSorter: no open sorter {}", h)),
        Some(s) => s.push(xs),
    }
}

pub fn stream_push_sorter(h: i64, xs: &Vec<u64>) {
    if let Err(e) = stream_sorter_push(h, xs) {
        panic!("{}", e);
    }
}

pub fn stream_next_sorted(h: i64) -> (bool, String, Vec<u64>) {
    let mut table = stream_sorters().lock().unwrap_or_else(|p| p.into_inner());
    match table.get_mut(&h) {
        None => (false, format!("nextSorted: no open sorter {}", h), Vec::new()),
        Some(s) => match s.next() {
            Ok(xs) => (true, String::new(), xs),
            Err(e) => (false, e, Vec::new()),
        },
    }
}

pub fn stream_close_sorter(h: i64) {
    stream_sorters().lock().unwrap_or_else(|p| p.into_inner()).remove(&h);
}
