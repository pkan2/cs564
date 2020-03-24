/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

BufMgr::~BufMgr() {
//flushes out all dirty pages 
  for (int i = 0; i < (int)numBufs; i++)
  {
        if(bufDescTable[i].dirty == true)
        {
            File* thisFile = bufDescTable[i].file;
            PageId thisPageNo = bufDescTable[i].pageNo;
            Page thisPage = bufPool[thisPageNo];
            bufStats.accesses++;
            thisFile -> writePage(thisPage);
            bufStats.diskwrites++;
            bufDescTable[i].dirty = false;
            hashTable -> remove(file, pageNo);
            bufDescTable[i].Clear();
        }
    }
//deallocates the buffer pool
	delete [] bufPool;
//deallocates the BufDesc table
  delete [] bufDescTable;
//deallocates the hashTable
  delete hashTable;
}


void BufMgr::advanceClock()
{
  //Advance clock to next frame in the buffer pool
  clockHand = (clockHand + 1) % numBufs;
}

/**
 * Allocate a free frame.
 *
 * @param frame   	Frame reference, frame ID of allocated frame returned via this variable
 * @throws BufferExceededException If no such buffer is found which can be allocated
 */
void BufMgr::allocBuf(FrameId & frame) 
{
    //the number of frames in the bufpool has been pinned
    int count = 0;
    bool allocated = false;
    while(count < (int)numBufs)
    {
       //begin using the clock algorithm
       if(bufDescTable[clockHand].valid == true)
       {
          if(bufDescTable[clockHand].refbit == false)
          {
            if(bufDescTable[clockHand].pinCnt == 0)
            {
              if(bufDescTable[clockHand].dirty == false)
              {
                 allocated = true;
                 frame = clockHand;
                 break;
              }
              else
              {
                 //flush page to disk
                 bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
                 bufStats.accesses++;
                 hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
                 bufDescTable[clockHand].Clear();
                 allocated = true;
                 frame = clockHand;
                 bufStats.diskwrites++;
                 break;
              }
            }
            else
            {
              //page has been pinned
              advanceClock();
              count++;
            }
          }
          else
          {
            //refbit has been set,clear refbit
            bufDescTable[clockHand].refbit= false;
            advanceClock();
          }
       }
       else
       {
         //not valid set
         allocated = true;
         frame = clockHand;
         break;
       }
    }
    if(allocated == false) 
    {
        //all buffer frames are pinned throws BufferExceededException 
        throw BufferExceededException();
    }
}

/**
 * Reads the given page from the file into a frame and returns the pointer to page.
 * If the requested page is already present in the buffer pool pointer to that frame is returned
 * otherwise a new frame is allocated from the buffer pool for reading the page.
 *
 * @param file   	File object
 * @param PageNo  Page number in the file to be read
 * @param page  	Reference to page pointer. Used to fetch the Page object in which requested page from file is read in.
 */
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
  try
    {
        //Case1: Page is in the buffer pool
        FrameId frame;
        hashTable->lookup(file, pageNo, frame);
        //set the appropriate refbit 
        bufDescTable[frame].refbit=true;
        //increment the pinCnt for the page
        bufDescTable[frame].pinCnt++;
        bufStats.accesses++;
        //return a pointer to the frame containing the page 
        page = &bufPool[frame];
    }
    catch(HashNotFoundException e)
    {
        //Case2: Page is not in the buffer pool
        FrameId frame;
        //call allocBuf() to allocate a buffer frame
        allocBuf(frame);  
        //call the method to read the page from disk into the buffer pool
        Page TargetPage = file->readPage(pageNo);
        bufStats.disreads++;
        bufPool[frame] = TargetPage;
        //insert the page into the hashtable
        hashTable->insert(file, pageNo, frame);
        page = &bufPool[frame];
        //invoke Set() on the frame to set it up properly
        bufDescTable[frame].Set(file, pageNo);
    }
}

/**
 * Unpin a page from memory since it is no longer required for it to remain in memory.
 *
 * @param file   	File object
 * @param PageNo  Page number
 * @param dirty		True if the page to be unpinned needs to be marked dirty
 * @throws  PageNotPinnedException If the page is not already pinned
 */
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
  try
    {
        FrameId frame;
        hashTable->lookup(file, pageNo, frame);
        //if the pin count is 0, throws PAGENOTPINNED
        if (bufDescTable[frame].pinCnt == 0)
        {
          throw PageNotPinnedException(file->filename(), pageNo, frame);
        } 
        //decrements the pinCnt of the frame containing (file,PageNo)
        bufDescTable[frame].pinCnt--;
        //if dirty==true, sets the dirty bit
        if (dirty == true) 
        {
          bufDescTable[frame].dirty = true;
        }
    }
    catch(HashNotFoundException e)
    {
      //does nothing if page is not found in the hash table look up
    }
}

/**
 * Writes out all dirty pages of the file to disk.
 * All the frames assigned to the file need to be unpinned from buffer pool before this function can be successfully called.
 * Otherwise Error returned.
 *
 * @param file   	File object
 * @throws  PagePinnedException If any page of the file is pinned in the buffer pool
 * @throws BadBufferException If any frame allocated to the file is found to be invalid
 */
void BufMgr::flushFile(const File* file) 
{
  for(int i=0; i< (int)numBufs; i++) {
        //scan pages belonging to the file
        if(bufDescTable[i].file==file) {
            PageId pageNo = bufDescTable[i].pageNo;
            FrameId frame;
            hashTable->lookup(file,pageNo,frame);
            //a) if the page is dirty
            if(bufDescTable[i].dirty == true) 
            { 
              //flush the page to the disk
              bufDescTable[i].file->writePage(bufPool[frame]); 
              //set the dirty bit for the page to false
              bufDescTable[i].dirty = false; 
              bufStats.diskwrites++;
            }
            //b) remove the page from the hashtable
            hashTable->remove(file,pageNo);
            //c) invoke the Clear() method of BufDesc for the page frame
            bufDescTable[i].Clear();
            //deal with the exception situation
            if(bufDescTable[i].pinCnt != 0)
            {
              //throws PagePinnedException if some page of the file is pinned
              throw PagePinnedException(file->filename(),bufDescTable[i].pageNo,frame);
            }
            if(bufDescTable[i].valid == false)
            {
              //throws BadBufferException if an invalid page belonging to the file is encountered
              throw BadBufferException(bufDescTable[i].frameNo,bufDescTable[i].dirty,bufDescTable[i].valid,bufDescTable[i].refbit); 
            }           
        }
    }
}

/**
 * Allocates a new, empty page in the file and returns the Page object.
 * The newly allocated page is also assigned a frame in the buffer pool.
 *
 * @param file   	File object
 * @param PageNo  Page number. The number assigned to the page in the file is returned via this reference.
 * @param page  	Reference to page pointer. The newly allocated in-memory Page object is returned via this reference.
 */
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
    //allocate an empty page in the specified file
    Page emptyPage = file->allocatePage();
    pageNo = emptyPage.page_number();
    FrameId frame;
    //call allocBuf to obtain a buffer pool frame
    allocBuf(frame);
    bufPool[frame] = emptyPage;// may not need
    bufStats.accesses++;
    page = &bufPool[frame];//may not need
    //an entry is inserted into the hashtable
    hashTable->insert(file, pageNo, frame);
    //invoke set() to set the table up properly
    bufDescTable[frame].Set(file, pageNo);
}

/**
 * Delete page from file and also from buffer pool if present.
 * Since the page is entirely deleted from file, its unnecessary to see if the page is dirty.
 *
 * @param file   	File object
 * @param PageNo  Page number
 */
void BufMgr::disposePage(File* file, const PageId PageNo)
{
   try
    {
        FrameId frame;
        hashTable->lookup(file, PageNo, frame);
        // Based on the question @316 on Piazza, there is no need to check
        // for the pin count.
        // The link is as followed:
        //      https://piazza.com/class/k5o4pp0u8fd5fw?cid=316
        /*
        if(bufDescTable[frame].pinCnt != 0) 
        {
          throw PagePinnedException(bufDescTable[frame].file->filename(), bufDescTable[frame].pageNo, bufDescTable[frame].frameNo);
        }
         */
        //free the frame
        bufDescTable[frame].Clear();
        //the correspondingly entry is removed from the hashTable
        hashTable->remove(file,PageNo);       
    }
    catch (HashNotFoundException e)
    {
        // Does nothing in this case, since this case is simply when the
        // deleting page is not allocated with a frame in the buffer pool.
        // And we need to do nothing in this case then.
    }
    //deleting the page from file
    file->deletePage(PageNo);  
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
