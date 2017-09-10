package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     * the file that stores the on-disk backing store for this heap
     * file.
     */
    private File f;
    private TupleDesc td;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableid = pid.getTableId();
        int pgno = pid.pageNumber();
        int pagesize = BufferPool.getPageSize();
        int offset = pgno * pagesize;
        byte[] rawdata = HeapPage.createEmptyPageData();
        try {
            InputStream in = new FileInputStream(f);
            in.skip(offset);
            in.read(rawdata);
            return new HeapPage(new HeapPageId(tableid, pgno), rawdata);
        } catch (FileNotFoundException e) {
            throw  new IllegalArgumentException("FileNotFoundException");
        } catch( IOException e)
        { throw new IllegalArgumentException("IOException");}
    }


    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pagesize = BufferPool.getPageSize();
        int offset = page.getId().pageNumber()*pagesize;
        byte[] rawdata = page.getPageData();
        try {
            OutputStream out = new FileOutputStream(f,true);
            out.write(rawdata);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("FileNotFoundException");
        } catch (IOException e){
            throw new IllegalArgumentException("IOException");
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int pagesize = BufferPool.getPageSize();
        long bytenums = f.length();
        return (int) bytenums/pagesize;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> modified = new ArrayList<>();
        int numpages = numPages();
        HeapPage currentpage;
        for (int i = 0;i<numpages;i++){
            HeapPageId pid = new HeapPageId(getId(),i);
            currentpage = (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
            if (currentpage.getNumEmptySlots()>0){
                currentpage.insertTuple(t);
                modified.add(currentpage);
                return modified;
            }
        }
        HeapPageId pid = new HeapPageId(getId(),numpages+1);
        currentpage = new HeapPage(pid,HeapPage.createEmptyPageData());
        currentpage.insertTuple(t);
        modified.add(currentpage);
        writePage(currentpage);
        // some code goes here
        return modified;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();
        HeapPage page =(HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
        if (pid.getTableId() == getId()) throw new DbException("not a same table");
        try {
            page.deleteTuple(t);
        } catch (DbException e){
            throw new DbException("tuple has deleted");
        }

        return page;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new myiterator(tid);
    }
    private class myiterator implements DbFileIterator{

        public TransactionId tid;
        public int tableid;
        public int endpid;
        public BufferPool bufpool;
        public Iterator<Tuple> iterator;
        public boolean open;

        public myiterator(TransactionId tid){
            this.tid = tid;
            this.tableid = getId();
            this.endpid = numPages()-1;
            this.bufpool = null;
            this.iterator = null;
            this.open = true;
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            if (this.bufpool == null) this.bufpool=Database.getBufferPool();
            try{
                this.bufpool = Database.getBufferPool();
                ArrayList<Tuple> result = new ArrayList<>();
                for (int i = 0;i<=endpid;i++){
                    PageId curpid = new HeapPageId(tableid,i);
                    HeapPage curpg = (HeapPage) bufpool.getPage(tid,curpid,Permissions.READ_ONLY);
                    Iterator<Tuple> curiter = curpg.iterator();
                    while (curiter.hasNext()){
                        result.add(curiter.next());
                    }
                }
                this.iterator = result.iterator();

            } catch (DbException e){
                throw new DbException("database error");
            } catch (TransactionAbortedException e){
                throw  new TransactionAbortedException();
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (bufpool == null) return false;
            return this.iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (bufpool == null)  throw  new NoSuchElementException();
                return this.iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (bufpool == null) throw  new IllegalStateException("bufferpool is null");
            open();
        }

        @Override
        public void close() {
            bufpool =null;
            this.iterator = null;
            this.open = false;
        }
    }
}

