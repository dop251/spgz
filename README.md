spgz ('Sparse gzip')
====

Spgz is a Go library for handling compressed files optimized for random read and write access.

A file is split into blocks of equal size. Each block can be stored either uncompressed or
compressed using gzip (it compresses the data and only stores it compressed if it saves at least
8KB). If the block is compressed a hole is punched at the reminder of the block and this is
how disk space is saved. The position of each block remains fixed allowing efficient random
access.

Because it uses Fallocate(FALLOC_FL_PUNCH_HOLE), it only works on filesystems that support it (see 
http://man7.org/linux/man-pages/man2/fallocate.2.html).

There is an overhead of one byte per block and a 4KB header.

This library was developed for the disk image backup tool.