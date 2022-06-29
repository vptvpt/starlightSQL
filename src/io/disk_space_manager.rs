use std::collections::HashMap;
use std::fs::try_exists;
use std::sync::atomic::AtomicU64;
use crate::io::partition_handle::PartitionHandle;

struct DiskSpaceManager {
    part_info: HashMap<usize, PartitionHandle>,
    db_dir: String,
    part_num_counter: AtomicU64,
}

impl DiskSpaceManager {
    pub fn new(db_dir: String) -> Self {
        let mut part_info = HashMap::new();
        let mut max_file_num = -1;
        if !try_exists(&db_dir).unwrap() {
            std::fs::create_dir_all(&db_dir).unwrap();
        } else {
            for entry in std::fs::read_dir(&db_dir).unwrap() {
                let entry = entry.unwrap();
                let file_name = entry.file_name();
                let file_name = file_name.to_str().unwrap();
                let file_num = file_name.parse::<isize>().unwrap();
                if file_num > max_file_num {
                    max_file_num = file_num;
                }
                let mut part_handle = PartitionHandle::new(file_num as usize);
                let path = db_dir.clone() + "/" + file_name;
                part_handle.open(&path);
                part_info.insert(file_num as usize, part_handle);
            }
        }
        DiskSpaceManager {
            part_info,
            db_dir,
            part_num_counter: AtomicU64::new(max_file_num as u64 + 1),
        }
    }
    fn alloc_partition(&mut self) -> usize {
        let part_num = self.part_num_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as usize;
        let mut part_handle = PartitionHandle::new(part_num);
        part_handle.open((self.db_dir.clone() + "/" + &part_num.to_string()).as_str());
        self.part_info.insert(part_num, part_handle);
        part_num
    }
    fn alloc_partition_with_num(&mut self, part_num: usize) -> usize {
        let mut part_handle = PartitionHandle::new(part_num);
        part_handle.open((self.db_dir.clone() + "/" + &part_num.to_string()).as_str());
        self.part_info.insert(part_num, part_handle);
        part_num
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_disk_space_manager() {
        let db_dir = "./db";
        let mut disk_space_manager = DiskSpaceManager::new(db_dir.to_string());
        let part_num = disk_space_manager.alloc_partition();
        assert_eq!(part_num, 0);
        let part_num = disk_space_manager.alloc_partition();
        assert_eq!(part_num, 1);
    }
    #[test]
    fn test_disk_space_manager_with_num() {
        let db_dir = "./db";
        let mut disk_space_manager = DiskSpaceManager::new(db_dir.to_string());
        let part_num = disk_space_manager.alloc_partition_with_num(3);
        assert_eq!(part_num, 3);
    }
}