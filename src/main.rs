#![feature(path_try_exists)]

pub mod io;

fn main() {
    let mut partition_handle = io::partition_handle::PartitionHandle::new(1);
    partition_handle.open("1.txt");
    partition_handle.print_master_page();
    println!("***********************************************************************");
    partition_handle.print_header_page(0);
    // partition_handle.alloc_page();
    partition_handle.free_all();
}

