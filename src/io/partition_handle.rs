use std::fs::{File, OpenOptions, try_exists};
use std::io::{Read, Seek, Write};
use std::u16;

const PAGE_SIZE: usize = 4096;
const MAX_HEADER_PAGES: usize = PAGE_SIZE / 2;
const DATA_PAGES_PER_HEADER: usize = PAGE_SIZE * 8;
const U8_PER_U16: usize = 2;

struct MasterPage {
    metadata: [u16; MAX_HEADER_PAGES],
}

impl MasterPage {
    fn new() -> MasterPage {
        MasterPage {
            metadata: [0; MAX_HEADER_PAGES],
        }
    }
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for i in 0..MAX_HEADER_PAGES {
            bytes.extend_from_slice(&self.metadata[i].to_le_bytes());
        }
        println!("{}", bytes.len());
        bytes
    }
    fn write_to_file(&self, file: &mut File) -> std::io::Result<()> {
        file.seek(std::io::SeekFrom::Start(0))?;
        file.write(&self.to_bytes())?;
        Ok(())
    }
    fn read_from_file(&self, file: &mut File) -> std::io::Result<MasterPage> {
        let mut bytes = [0u8; PAGE_SIZE];
        file.seek(std::io::SeekFrom::Start(0))?;
        file.read_exact(&mut bytes)?;
        let mut metadata = [0; MAX_HEADER_PAGES];
        for i in 0..MAX_HEADER_PAGES {
            metadata[i] = u16::from_le_bytes(bytes[i * U8_PER_U16..(i + 1) * U8_PER_U16].try_into().unwrap());
        }
        Ok(MasterPage { metadata })
    }
    fn up(&mut self, header_index: usize) {
        self.metadata[header_index] += 1;
    }
    fn down(&mut self, header_index: usize) {
        self.metadata[header_index] -= 1;
    }
}

pub struct PartitionHandle {
    master_page: MasterPage,
    header_pages: Vec<Vec<u8>>,
    file: Option<File>,
    part_num: usize,
}

impl PartitionHandle {
    pub(crate) fn new(part_num: usize) -> Self {
        PartitionHandle {
            master_page: MasterPage::new(),
            header_pages: vec![vec![0; PAGE_SIZE]; MAX_HEADER_PAGES],
            file: None,
            part_num,
        }
    }
    pub(crate) fn open(&mut self, filename: &str) {
        if !try_exists(filename).unwrap() {
            self.file = Option::from(File::create(filename).unwrap());
            self.master_page.write_to_file(self.file.as_mut().unwrap()).unwrap();
            //TODO: lazy initialization of header pages
            for i in 0..MAX_HEADER_PAGES {
                self.write_header_page(i as usize);
            }
        } else {
            self.file = Option::from(OpenOptions::new().read(true).write(true).truncate(false).open(filename).unwrap());
            // self.read_master_page();
            self.read_header_page();
            self.master_page = self.master_page.read_from_file(self.file.as_mut().unwrap()).unwrap();
        };
    }
    pub fn alloc_page(&mut self) -> usize {
        let mut header_index = None;
        let mut data_page_index = None;
        for i in 0..MAX_HEADER_PAGES {
            if self.master_page.metadata[i] < DATA_PAGES_PER_HEADER as u16 {
                header_index = Some(i);
                break;
            }
        }
        if header_index == None {
            panic!("No more space for new data page");
        }
        for i in 0..PAGE_SIZE {
            let byte = self.header_pages[header_index.unwrap()][i];
            for j in 0..8 {
                let bit = byte & (1 << j);
                if bit == 0u8 {
                    data_page_index = Some(i * 8 + j);
                    break;
                }
            }
            if data_page_index != None {
                break;
            }
        }
        if data_page_index == None {
            panic!("No more space for new data page");
        }
        println!("alloc_page: header_index: {}, data_page_index: {}", header_index.unwrap(), data_page_index.unwrap());
        self._alloc_page(header_index.unwrap(), data_page_index.unwrap())
    }
    fn _alloc_page(&mut self, header_index: usize, data_page_index: usize) -> usize {
        self.header_pages[header_index][data_page_index / 8] |= 1 << (data_page_index % 8);
        self.master_page.up(header_index);
        let page_num = header_index * DATA_PAGES_PER_HEADER + data_page_index;
        let _vpn = self.part_num * 10000000000 + page_num;
        self.master_page.write_to_file(self.file.as_mut().unwrap()).unwrap();
        self.write_header_page(header_index);
        page_num
    }
    pub fn free_page(&mut self, page_num: usize) {
        let header_index = page_num / DATA_PAGES_PER_HEADER;
        let data_page_index = page_num % DATA_PAGES_PER_HEADER;
        let already_free = self.header_pages[header_index][data_page_index / 8] & (1 << (data_page_index % 8));
        if already_free == 0 {
            panic!("Page already free");
        }
        //取非
        self.header_pages[header_index][data_page_index / 8] &= !(1 << (data_page_index % 8));
        self.master_page.down(header_index);
        self.master_page.write_to_file(self.file.as_mut().unwrap()).unwrap();
        self.write_header_page(header_index);
    }
    pub fn free_all(&mut self) {
        for i in 0..MAX_HEADER_PAGES {
            for j in 0..PAGE_SIZE {
                self.header_pages[i][j] = 0;
            }
        }
        self.master_page.metadata = [0; MAX_HEADER_PAGES];
        self.master_page.write_to_file(self.file.as_mut().unwrap()).unwrap();
        for i in 0..MAX_HEADER_PAGES {
            self.write_header_page(i);
        }
    }
    fn is_not_allocated(&self, page_num: usize) -> bool {
        let header_index = page_num / DATA_PAGES_PER_HEADER;
        let data_page_index = page_num % DATA_PAGES_PER_HEADER;
        let already_free = self.header_pages[header_index][data_page_index / 8] & (1 << (data_page_index % 8));
        already_free == 0
    }
    pub fn read_page(&mut self, page_num: usize) -> Vec<u8> {
        if self.is_not_allocated(page_num) {
            panic!("Page not allocated");
        }
        let mut bytes = vec![0u8; PAGE_SIZE];
        self.file.as_mut().unwrap().seek(std::io::SeekFrom::Start(PartitionHandle::data_page_offset(page_num) as u64)).unwrap();
        self.file.as_mut().unwrap().read_exact(&mut bytes).unwrap();
        bytes
    }
    pub fn write_page(&mut self, page_num: usize, buf: &[u8]) {
        if self.is_not_allocated(page_num) {
            panic!("Page not allocated");
        }
        self.file.as_mut().unwrap().seek(std::io::SeekFrom::Start(PartitionHandle::data_page_offset(page_num) as u64)).unwrap();
        self.file.as_mut().unwrap().write_all(buf).unwrap();
    }
    fn data_page_offset(page_num: usize) -> usize {
        let other_headers = page_num / DATA_PAGES_PER_HEADER;
        (2 + other_headers + page_num) * PAGE_SIZE
    }
    fn write_header_page(&self, header_index: usize) {
        let header_page_offset = self.header_page_offset(header_index);
        self.file.as_ref().unwrap().seek(std::io::SeekFrom::Start(header_page_offset as u64)).unwrap();
        self.file.as_ref().unwrap().write_all(&self.header_pages[header_index]).unwrap();
    }
    fn read_header_page(&mut self) {
        for i in 0..MAX_HEADER_PAGES {
            let header_page_offset = self.header_page_offset(i);
            self.file.as_ref().unwrap().seek(std::io::SeekFrom::Start(header_page_offset as u64)).unwrap();
            self.file.as_ref().unwrap().read_exact(&mut self.header_pages[i]).unwrap();
        }
    }
    fn header_page_offset(&self, header_index: usize) -> usize {
        (PAGE_SIZE * (header_index * (DATA_PAGES_PER_HEADER + 1) + 1)) as usize
    }
    pub(crate) fn print_master_page(&self) {
        println!("master page : {:?}", self.master_page.metadata);
    }
    pub(crate) fn print_header_page(&self, header_index: usize) {
        println!("{:?}", self.header_pages[header_index]);
    }
}