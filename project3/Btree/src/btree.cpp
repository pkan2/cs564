/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/page_not_pinned_exception.h"

#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"

//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------
/**
 * Constructor
 *
 * The constructor first checks if the specified index file exists.
 * And index file name is constructed by concatenating the relational name with
 * the offset of the attribute over which the index is built.
 *
 * If the index file exists, the file is opened.
 * Else, a new index file is created.
 *
 * @param relationName The name of the relation on which to build the index. 
 * @param outIndexName The name of the index file
 * @param bufMgrIn The instance of the global buffer manager.
 * @param attrByteOffset The byte offset of the attribute in the tuple on which to build the index.
 * @param attrType The data type of the attribute we are indexing.
 */
BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
    this -> bufMgr = bufMgrIn;
    this -> attributeType = attrType;
    this -> attrByteOffset = attrByteOffset;
    
    // update the value of node occupancy and leaf node occupancy
    // this part of code is following the variable INTARRAYLEAFSIZE and
    // INTARRAYNONLEAFSIZE from the btree.h
    switch(attrType){
        case INTEGER:
            this -> nodeOccupancy = (Page::SIZE - sizeof(int) - sizeof(PageId)) / (sizeof(int) + sizeof(PageId));
            this -> leafOccupancy = (Page::SIZE - sizeof(PageId)) / (sizeof(int) + sizeof(RecordId));
            break;
        case DOUBLE:
            this -> nodeOccupancy = (Page::SIZE - sizeof(double) - sizeof(PageId)) / (sizeof(double) + sizeof(PageId));
            this -> leafOccupancy = (Page::SIZE - sizeof(PageId)) / (sizeof(double) + sizeof(RecordId));
            break;
        case STRING:
            // the size of string record is provided in the instruction file
            this -> nodeOccupancy = (Page::SIZE - sizeof(char[64]) - sizeof(PageId)) / (sizeof(char[64]) + sizeof(PageId));
            this -> leafOccupancy = (Page::SIZE - sizeof(PageId)) / (sizeof(char[64]) + sizeof(RecordId));
            break;
    }

    // initialize all the variables for the scanning:
    this -> lowValInt = -1;
    this -> highValInt = -1;
    this -> lowValDouble = -1;
    this -> highValDouble = -1;
    this -> lowValString = "";
    this -> highValString = "";
    this -> scanExecuting = false;
    // we define the initial value of nextEntry as -1, which is always
    // an invalid value for index.
    // also, we specify the case where nextEntry = -2, to present the
    // case of scan complete.
    this -> nextEntry = -1;
    this -> currentPageNum = Page::INVALID_NUMBER;
    this -> currentPageData = NULL;
    // the enum variable usually starts value from 1. Thus, -1 is an
    // invalid value for enum variable
    this -> lowOp = (Operator)-1;
    this -> highOp = (Operator)-1;
    
    // find the index file name
    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    // indexName is the name of the index file
    std::string indexName = idxStr.str();
    outIndexName = indexName;
    
    // check if the index file is already existing or not
    if(File::exists(indexName)){
        BlobFile* newfile = new BlobFile(outIndexName, false);
        File * file = (File*) newfile;
        // check whether the existing file matching with our input info
        // find the meta page, which is specified to be 1
        PageId metaPageId = 1;
        Page * metaPage;
        this -> bufMgr -> readPage(file, metaPageId, metaPage);
        IndexMetaInfo * metaInfo = (IndexMetaInfo *) metaPage;
        // check the attribute in the meta page
        if(!(metaInfo -> relationName == relationName
           && metaInfo -> attrByteOffset == attrByteOffset
           && metaInfo -> attrType == attrType)){
            this -> bufMgr -> unPinPage(file, metaPageId, false);
            throw BadIndexInfoException("Index file exists but values in metapage not match.");
        }
        this -> headerPageNum = metaPageId;
        this -> rootPageNum = metaInfo -> rootPageNo;
        this -> file = file;
        this -> bufMgr -> unPinPage(this -> file, metaPageId, false);
        return;
    }
    
    BlobFile* newFile = new BlobFile(outIndexName, true);
    this -> file = (File *) newFile;
    // create the metadata page
    PageId metaPageId;
    Page * metaPage;
    this -> bufMgr -> allocPage(this -> file, metaPageId, metaPage);
    IndexMetaInfo * metaInfo = (IndexMetaInfo *) metaPage;
    // write the input attributes into the metaPage
    strcpy(metaInfo -> relationName, relationName.c_str());
    metaInfo -> attrByteOffset = attrByteOffset;
    metaInfo -> attrType = attrType;
    // assign the meta page id to the private attribute
    this -> headerPageNum = metaPageId;
    // create the root page and update the attribute for root page
    // update the root page attribute in the metaPage
    PageId rootPageId;
    Page * rootPage;
    this -> bufMgr -> allocPage(this -> file, rootPageId, rootPage);
    // initialize value of the vars in the rootPage, which is firstly
    // initialized as a leaf node.
    ((LeafNodeInt *) rootPage) -> slotTaken = 0;
    ((LeafNodeInt *) rootPage) -> rightSibPageNo = Page::INVALID_NUMBER;
    this -> bufMgr -> unPinPage(this -> file, rootPageId, true);
    metaInfo -> rootPageNo = rootPageId;
    this -> rootPageNum = rootPageId;
    // the initial index page for root is actually a leaf node, since
    // this is the only node as the start.
    this -> rootIsLeaf = true;
    this -> bufMgr -> unPinPage(this -> file, metaPageId, true);
    // scan the relation and insert into the index file
    FileScan * fileScan = new FileScan(relationName, bufMgrIn);
    try
    {
        RecordId scanRid;
        while(1)
        {
            fileScan -> scanNext(scanRid);
            std::string recordStr = fileScan -> getRecord();
            const char *record = recordStr.c_str();
            // as mentioned in the instruction, the data type of key
            // in this assignment will only be integer.
            int key = *((int *)(record + attrByteOffset));
            this -> insertEntry(&key, scanRid);
        }
    }
    catch(EndOfFileException e)
    {
        // this case means reaching the end of the relation file.
        //std::cout << "Read all records" << std::endl;
    }
    delete fileScan;

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------
/**
 * Destructor
 * Perform any cleanup that may be necessary, 
 * including clearing up any state variables,
 * unpinning any B+ Tree pages that are pinned, 
 * and flushing the index file (by calling bufMgr->flushFile()).
 *
 * Note that this method does not delete the index file! 
 * But, deletion of the file object is required, 
 * which will call the destructor of File class causing the index file to be closed.
 */
BTreeIndex::~BTreeIndex()
{
    // end all scans
    if(scanExecuting) endScan();
    // unpin all the pages from the index file, i.e. BlobFile
    // Flush this index file
    // the unpin process can be guaranteed by the endScan method already
    /*
    // DEBUG ONLY
     this -> bufMgr -> printSelf();
     */
    
    this -> bufMgr -> flushFile(this -> file);
    
    // delete the index file
    delete this -> file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
/**
 * Insert a new entry using the pair <value,rid>.
 * @param key			A pointer to the value(integer we want to insert)
 * @param rid			The corresponding record id of the tuple in the base relation
 **/
const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
    std::vector<PageId> searchPath;
    if(this -> rootIsLeaf == true){
        // this is the case when the root is a leaf index page already,
        // then we can just try to insert into this root page first
        this -> insertLeafNode(this -> rootPageNum, key, rid, searchPath);
    }
    else{
        // this is the case when the root page is not a leaf.
        // Then, we need to look for a potential leaf page to insert
        // this target key value in.
        PageId currPageId = Page::INVALID_NUMBER;
        this -> searchLeafPageWithKey(key, currPageId, this -> rootPageNum, searchPath);
        // insert the (key, rid) pair into this potential leaf node
        this -> insertLeafNode(currPageId, key, rid, searchPath);
    }
}

/**
 * Insert a new (key, rid) pair into a leaf node
 * @param pid: the PageId of the potential leaf node to insert into
 * @param key: a pointer to the value(integer we want to insert)
 * @param rid: The corresponding record id of the tuple in the base relation
 * @param searchPath: a vector of PageId contains all the PageId of the pages we have
 *  visited along our search path. The purpose of this vector is to benefit our insert later.
 *  Remark: the searchPath does not contain the pageId of this current node.
 */
const void BTreeIndex::insertLeafNode(const PageId pid, const void *key, const RecordId rid, std::vector<PageId> & searchPath){
    Page * currPage;
    this -> bufMgr -> readPage(this -> file, pid, currPage);
    LeafNodeInt * currLeafPage = (LeafNodeInt*) currPage;
    // check whether there is enough space to insert into this
    // current leaf index page
    if(currLeafPage -> slotTaken < this -> leafOccupancy){
        // the case where there is still available space in this current
        // leaf index page
        // we may need to reorder the current array unit storing in this
        // leaf page, in order to make sure the keys in this leaf page
        // is sorted.
        int i;
        for(i = 0; i < currLeafPage -> slotTaken; ++i){
            // shift all the slots with key value larger than this
            // target key into the slots upper by 1.
            if(currLeafPage -> keyArray[currLeafPage -> slotTaken - 1 - i] >  *((int *) key)){
                currLeafPage -> keyArray[currLeafPage -> slotTaken - i] = currLeafPage -> keyArray[currLeafPage -> slotTaken - 1 - i];
                currLeafPage -> ridArray[currLeafPage -> slotTaken - i] = currLeafPage -> ridArray[currLeafPage -> slotTaken - 1 - i];
            }
            // the only case in the else is where the key value in
            // the slot "currLeafPage -> slotTaken - 1 - i" is less than
            // the key value of the target, since we assume that
            // there is no duplication of key value.
            // then we can store the new target pair into the slot
            // "currLeafPage -> slotTaken - i"
            else{
                break;
            }
        }
        currLeafPage -> keyArray[currLeafPage -> slotTaken - i] = *((int *) key);
        currLeafPage -> ridArray[currLeafPage -> slotTaken - i] = rid;
        // update the amount of slots being taken up in the leaf node
        currLeafPage -> slotTaken += 1;
        // unpin this leaf index page
        this -> bufMgr -> unPinPage(this -> file, pid, true);
        return;
    }
    else{
        // this is the case where there is no more space to insert in
        // a new (key, rid) pair in this current leaf page
        // unpin the current leaf page pinned in this function
        this -> bufMgr -> unPinPage(this -> file, pid, false);
        // we need to split this leaf page up into two parts
        this -> splitLeafNode(pid, key, rid, searchPath);
    }
}
/**
 * Split up a leaf index page.
 * @param pid: the page id of the current leaf node, which is needed to be splitted
 * @param key: a pointer to the value of the key that we are looking for
 * @param rid: The corresponding record id of the tuple in the base relation
 * @param searchPath: a vector of PageId contains all the PageId of the pages we have
 *  visited along our search path. The purpose of this vector is to benefit our insert later.
 *  Remark: the searchPath does not contain the pageId of this current node.
 */
const void BTreeIndex::splitLeafNode(PageId pid, const void *key,  const RecordId rid, std::vector<PageId> & searchPath){
    Page * currPage;
    this -> bufMgr -> readPage(this -> file, pid, currPage);
    LeafNodeInt * currLeafPage = (LeafNodeInt*) currPage;
    Page * newPage;
    PageId newPageId;
    this -> bufMgr -> allocPage(this -> file, newPageId, newPage);
    LeafNodeInt * newLeafPage = (LeafNodeInt*) newPage;
    // initialize the variable in this new leaf node
    newLeafPage -> slotTaken = 0;
    // we will let the new leaf page to be the one with larger key values
    // and let the current leaf node with smaller key values
    // i.e. the new leaf page is the right page of the upper key
    // and the current leaf page changed from the right page of the upper key into its left page.
    newLeafPage -> rightSibPageNo = currLeafPage -> rightSibPageNo;
    currLeafPage -> rightSibPageNo = newPageId;
    // we need to split this current leaf page up into two parts,
    // by the sizes of leafOccupancy / 2 and leafOccupancy / 2 + 1
    bool newKeyInserted = false; // var to keep track whether the new
    // key has been inserted or not.
    int threshold; // # of keys to split up the non-leaf node
    
    if(this -> leafOccupancy % 2 == 0){
        threshold = this -> leafOccupancy / 2;
    }
    else{
        threshold = this -> leafOccupancy / 2 + 1;
    }
    
    for(int i= 0; i < this -> leafOccupancy; ++i){
        if(currLeafPage -> keyArray[this -> leafOccupancy - 1 - i] > *((int *)key)){
            if(newLeafPage -> slotTaken <  this -> leafOccupancy + 1 - threshold){
                // this is the case that we should still insert into
                // the new leaf node
                newLeafPage -> keyArray[this -> leafOccupancy  + 1 - threshold - 1 - i] = currLeafPage -> keyArray[this -> leafOccupancy - 1 - i];
                newLeafPage -> ridArray[this -> leafOccupancy + 1 - threshold - 1 - i] = currLeafPage -> ridArray[this -> leafOccupancy - 1 - i];
                newLeafPage -> slotTaken += 1;
                currLeafPage -> slotTaken -= 1;
            }
            else{
                // the new leaf has been filled up to the specified amount
                // already
                
                // shift up the slots in this current leaf page, to make
                // space for the new key value
                currLeafPage -> keyArray[this -> leafOccupancy - i] =  currLeafPage -> keyArray[this -> leafOccupancy - 1 - i];
                currLeafPage -> ridArray[this -> leafOccupancy - i] =  currLeafPage -> ridArray[this -> leafOccupancy - 1 - i];
               
                // this is the case where all the keys in the original current leaf are actually larger than the new inserted key. Then, as i is only in range of the amount of total amount of keys in the original current leaf, we never get a chance to insert the new key value.
                if(i + 1 == this -> leafOccupancy){
                    currLeafPage -> keyArray[this -> leafOccupancy - 1 - i] = *((int*) key);
                    currLeafPage -> ridArray[this -> leafOccupancy - 1 - i] = rid;
                    currLeafPage -> slotTaken += 1;
                    newKeyInserted = true;
                }
            }
        }
        else{
            if(newKeyInserted == false){
                // this is the first time we meet an exisitng key
                // larger than the new key value.
                // We should insert in the new key first.
                if(newLeafPage -> slotTaken <  this -> leafOccupancy + 1 - threshold){
                    newLeafPage -> keyArray[this -> leafOccupancy + 1 - threshold - 1 - i] = *((int*)key);
                    newLeafPage -> ridArray[this -> leafOccupancy + 1 - threshold - 1 - i] = rid;
                    newLeafPage -> slotTaken += 1;
                    newKeyInserted = true;
                }
                else{
                   
                    // DEBUG ONLY
                    if(newLeafPage -> slotTaken !=  this -> leafOccupancy + 1 - threshold)
                    std::cout << "the slotTaken value of the new leaf is wrong at (2)! Current slotTaken amount is " << newLeafPage -> slotTaken << " expect to be " << this -> leafOccupancy - threshold << std::endl;
                    if(currLeafPage -> slotTaken != threshold - 1)
                    std::cout << "the slotTaken value of the current leaf is wrong at (2)! Current slotTaken amount is " << currLeafPage -> slotTaken << " expect to be " <<  threshold << std::endl;
                    
                    
                    // the new key has to be put in the current leaf page
                    currLeafPage -> keyArray[this -> leafOccupancy - i] =  *((int*)key);
                    currLeafPage -> ridArray[this -> leafOccupancy - i] =  rid;
                    currLeafPage -> slotTaken += 1;
                    newKeyInserted = true;
                }
            }
            // this is the part under when the new key has been
            // inserted already
            // now, we have to re-position the original (leafOccupancy - i - 1) - th slot of the current leaf node
            if(newLeafPage -> slotTaken <  this -> leafOccupancy + 1 - threshold){
                newLeafPage -> keyArray[this -> leafOccupancy + 1 - threshold - 2 - i] = currLeafPage -> keyArray[this -> leafOccupancy - i - 1];
                newLeafPage -> ridArray[this -> leafOccupancy + 1 - threshold - 2 - i] = currLeafPage -> ridArray[this -> leafOccupancy - i - 1];
                newLeafPage -> slotTaken += 1;
                currLeafPage -> slotTaken -= 1;
            }
            else{
                
                // DEBUG ONLY
                if(newLeafPage -> slotTaken !=  this -> leafOccupancy + 1 - threshold)
                std::cout << "the slotTaken value of the new leaf is wrong at (3)! Current slotTaken amount is " << newLeafPage -> slotTaken << " expect to be " << this -> leafOccupancy - threshold << std::endl;
                 if(currLeafPage -> slotTaken != threshold)
                 std::cout << "the slotTaken value of the current leaf is wrong at (3)! Current slotTaken amount is " << currLeafPage -> slotTaken << " expect to be " <<  threshold + 1<< std::endl;
                
                // this is the case where the filling of the new leaf is done
                // since the new key has been inserted already as well
                // nothing needed to be done in this case.
                break;
            }
        }
    }
    
    int pushup = newLeafPage -> keyArray[0]; // the key value needed to push up into the upper layer non-leaf node
    this -> bufMgr -> unPinPage(this -> file, pid, true);
    this -> bufMgr -> unPinPage(this -> file, newPageId, true);
    // check whether this current page is actually a root
    if(searchPath.size() == 0){
        // case when this current page is a root. Then, we need to
        // create a new non-leaf root.
        this -> createAndInsertNewRoot(&pushup, pid, newPageId, 1);
    }
    else{
        // case when this current page is not a root.
        // get the upper level non-leaf parent node
        PageId parentId = searchPath[searchPath.size() - 1];
        // delete the parentId from the searchPath, to generate the search path for the parentId
        searchPath.erase(searchPath.begin() + searchPath.size() - 1);
        this -> insertNonLeafNode(parentId, &pushup, newPageId, searchPath, true);
    }
}

/**
 * This function helps create a new non-leaf root, with inserting the pushup values into this root.
 * @param key: the new key needed to insert in to this non-leaf root
 * @param leftPageId: the pageId on the left side of this new key
 * @param rightPageId: the pageId on the right side of this new key
 * @param level: the level of this non-leaf root page
 */
const void BTreeIndex::createAndInsertNewRoot(const void *key, const PageId leftPageId, const PageId rightPageId, int level){
    PageId rootId;
    Page * rootPage;
    // allocate a page for the new non-leaf root
    this -> bufMgr -> allocPage(this -> file, rootId, rootPage);
    NonLeafNodeInt * nonLeafRootPage = (NonLeafNodeInt*) rootPage;
    // initiate and update the var for this new non-leaf root
    nonLeafRootPage -> level = level;
    nonLeafRootPage -> keyArray[0]= *((int*) key);
    nonLeafRootPage -> pageNoArray[0] = leftPageId;
    nonLeafRootPage -> pageNoArray[1] = rightPageId;
    nonLeafRootPage -> slotTaken += 1;
    this -> bufMgr -> unPinPage(this -> file, rootId, true);
    // update the private var and the vars in the meta page
    this -> rootIsLeaf = false;
    this -> rootPageNum = rootId;
    Page * metaPage;
    this -> bufMgr -> readPage(this -> file, this -> headerPageNum, metaPage);
    IndexMetaInfo * metaInfo = (IndexMetaInfo*) metaPage;
    metaInfo -> rootPageNo = rootId;
    // unpin this meta page
    this -> bufMgr -> unPinPage(this -> file, this -> headerPageNum, true);
}

/**
 * This function helps insert the pushup key from lower level into the upper level non-leaf node.
 * @param pid: the PageId of this non-leaf node
 * @param key: the new key or the pushup-ed key from lower level
 * @param leftPageId: the pageId of the newly created page in the lower level and need to insert this pageId on the left side of the new key.
 * Remark: if this key is actually inserted from a lower leaf page, then the newly created pageId is actually for the right pageId.
 * @param searchPath: the search path leading toward this current non-leaf node.
 * Remark: the searchPath does not contain the pageId of this current node.
 * @param: fromLeaf: is the bool var, true means inserting up from a leaf node. false means from a
 * nonleaf node.
 */
const void BTreeIndex::insertNonLeafNode(PageId pid, const void *key, const PageId leftPageId, std::vector<PageId> searchPath, bool fromLeaf){
    Page * currPage;
    this -> bufMgr -> readPage(this -> file, pid, currPage);
    NonLeafNodeInt * currNonLeafPage = (NonLeafNodeInt*) currPage;
    // check whether there is enough space to insert into this
    // current non-leaf index page
    if(currNonLeafPage -> slotTaken < this -> nodeOccupancy){
        // the case where there is still available space in this current
        // non-leaf index page
        // we may need to reorder the current array unit storing in this
        // non-leaf page, in order to make sure the keys in this non-leaf page
        // is sorted.
        int i;
        // One observation is that the newly inserted pageId corresponding to the page with keys smaller than this new key.
        // Therefore, the newly inserted pageId will be on the left side of the new key value, which means this pageId will be at the same index in the pageNoArray as the index of the new key in the keyArray.
        // shift the slot for the pageId corresponding to the greatest key upper by 1. We know that the newly inserted key will at most belong to the page pointed by the pageId corresponding to the greatest key.
        // Thus, the newly inserted pageId will be on the left of this pageId corresponding to the greatest key for sure.
        currNonLeafPage -> pageNoArray[currNonLeafPage -> slotTaken + 1] =  currNonLeafPage -> pageNoArray[ currNonLeafPage -> slotTaken];
        for(i = 0; i <  currNonLeafPage -> slotTaken; ++i){
            // shift all the slots with key value larger than this
            // target key into the slots upper by 1.
            if( currNonLeafPage -> keyArray[ currNonLeafPage -> slotTaken - 1 - i] >  *((int *) key)){
                 currNonLeafPage -> keyArray[ currNonLeafPage -> slotTaken - i] =  currNonLeafPage -> keyArray[ currNonLeafPage -> slotTaken - 1 - i];
                 currNonLeafPage -> pageNoArray[ currNonLeafPage -> slotTaken - i] =  currNonLeafPage -> pageNoArray[ currNonLeafPage -> slotTaken - 1 - i];
            }
            // the only case in the else is where the key value in
            // the slot "currLeafPage -> slotTaken - 1 - i" is less than
            // the key value of the target, since we assume that
            // there is no duplication of key value.
            // then we can store the new target pair (key, PageId) into the slot
            // " currNonLeafPage -> slotTaken - i"
            else{
                break;
            }
        }
        if(fromLeaf == false){
         currNonLeafPage -> keyArray[ currNonLeafPage -> slotTaken - i] = *((int *) key);
         currNonLeafPage -> pageNoArray[ currNonLeafPage -> slotTaken - i] = leftPageId;
        }
        else{
            // if the new key is inserted from a leaf node, then the new
            // pageId param is acutally the rightPageId
            PageId CorrectLeftPageId = currNonLeafPage -> pageNoArray[ currNonLeafPage -> slotTaken - i + 1];
            currNonLeafPage -> pageNoArray[ currNonLeafPage -> slotTaken - i + 1] = leftPageId;
            currNonLeafPage -> keyArray[ currNonLeafPage -> slotTaken - i] = *((int *) key);
            currNonLeafPage -> pageNoArray[ currNonLeafPage -> slotTaken - i] = CorrectLeftPageId;
        }
        // update the amount of slots being taken up in the
        // leaf index page
         currNonLeafPage -> slotTaken += 1;
        // unpin this non-leaf index page
        this -> bufMgr -> unPinPage(this -> file, pid, true);
        return;
    }
    else{
        // this is the case where there is no more space to insert in
        // a new (key, leftpageId) pair in this current non-leaf page
        // unpin the current leaf page pinned in this function
        this -> bufMgr -> unPinPage(this -> file, pid, false);
        // we need to split this non-leaf page up into two parts
        this -> splitNonLeafNode(pid, key, leftPageId, searchPath, fromLeaf);
    }
}

/**
 * Split up a non-leaf index page.
 * @param pid: the page id of the current non-leaf node, which is needed to be splitted
 * @param key: the new key needed to be inserted
 * @param leftPageId: the pageId of the newly created page in the lower level and need to insert this pageId on the left side of the new key
 * Remark: if this key is actually inserted from a lower leaf page, then the newly created pageId is actually for the right pageId.
 * @param searchPath: a vector of PageId contains all the PageId of the pages we have
 *  visited along our search path. The purpose of this vector is to benefit our insert later.
 *  Remark: the searchPath does not contain the pageId of this current node.
 *   @param: fromLeaf: is the bool var, true means inserting up from a leaf node. false means from a nonleaf node.
 */
const void BTreeIndex::splitNonLeafNode(PageId pid, const void *key,  const PageId leftPageId, std::vector<PageId> & searchPath, bool fromLeaf){
    // most part of this function should be similar to the splitLeafNode function. However, in this splitNonLeaf case, we don't copy,i.e. keep, the pushup value any more.
    // TODO.
    Page * currPage;
    this -> bufMgr -> readPage(this -> file, pid, currPage);
    NonLeafNodeInt * currNonLeafPage = (NonLeafNodeInt*) currPage;
    Page * newPage;
    PageId newPageId;
    this -> bufMgr -> allocPage(this -> file, newPageId, newPage);
    NonLeafNodeInt * newNonLeafPage = (NonLeafNodeInt*) newPage;
    // initialize the variable in this new leaf node
    newNonLeafPage -> slotTaken = 0;
    newNonLeafPage -> level = currNonLeafPage -> level;
    // we need to split this current non-leaf page up into two parts,
    // by the sizes of nodeOccupancy / 2 and nodeOccupancy / 2 + 1
    bool newKeyInserted = false; // var to keep track whether the new
    // key has been inserted or not.
    int pushup; // the key value needed to push up into the upper layer non-leaf node
    int threshold; // # of keys to split up the non-leaf node
    if(this -> nodeOccupancy % 2 == 0){
        threshold = this -> nodeOccupancy / 2;
    }
    else{
        threshold = this -> nodeOccupancy / 2 + 1;
    }
    for(int i= 0; i < this -> nodeOccupancy; ++i){
        if( currNonLeafPage -> keyArray[i] < *((int *)key)){
            if(newNonLeafPage -> slotTaken < threshold){
                newNonLeafPage -> keyArray[i] =  currNonLeafPage -> keyArray[i];
                newNonLeafPage -> pageNoArray[i] =  currNonLeafPage -> pageNoArray[i];
                newNonLeafPage -> slotTaken += 1;
                 currNonLeafPage -> slotTaken -= 1;
            }
            // this is the breaking up of the currNonLeafPage's keys,
            // which is the first key left in the current non-leaf page,
            // i.e. the first key that cannot be inserted in the new non-leaf node.
            // However, in the non-leaf node pushup, we don't need to
            // copy the element needed to push up in our current non-leaf page.
            // Therefore, we will not store it in the current non-leaf page.
            // Instead, we will directly push up and insert this key into the upper level parent non-leaf page.
            else if(newNonLeafPage -> slotTaken == threshold){
                // push up this key to break up the new non-leaf Node and the current non-leaf node.
                pushup =  currNonLeafPage -> keyArray[i];
                // Even though we don't copy or store the value of this key in any non-leaf node in this level, we cannot
                // lose the pageId that this key is corresponding to.
                // We will store the pageId corresponding to this pushup key at the end of the new non-leaf node.
                newNonLeafPage -> pageNoArray[newNonLeafPage -> slotTaken] =  currNonLeafPage -> pageNoArray[i];
                 currNonLeafPage -> slotTaken -= 1;
            }
            else{
                // shift down the rest slots in this current non-leaf page (there have newNonLeafPage -> slotTaken + 1 slots being removed from this current non-leaf page in the front already.)
                 currNonLeafPage -> keyArray[i - newNonLeafPage -> slotTaken - 1] =   currNonLeafPage -> keyArray[i];
                 currNonLeafPage -> pageNoArray[i - newNonLeafPage -> slotTaken - 1] =   currNonLeafPage -> pageNoArray[i];
            }
        }
        else{
            if(newKeyInserted == false){
                // this is the first time we meet an exisitng key
                // larger than the new key value.
                // We should insert in the new key first.
                if(newNonLeafPage -> slotTaken < threshold){
                    newNonLeafPage -> keyArray[i] = *((int *)key);
                    // it is remarked the right pageId of this new key
                    // has not been changed and it should still be pointing to the slots that this new key used to belong to.
                    if(fromLeaf == false){
                        newNonLeafPage -> pageNoArray[i] = leftPageId;
                    }
                    else{
                        PageId trueLeftPageId = currNonLeafPage -> pageNoArray[i];
                        currNonLeafPage -> pageNoArray[i] = leftPageId;
                        newNonLeafPage -> pageNoArray[i] = trueLeftPageId;
                    }
                    newNonLeafPage -> slotTaken += 1;
                    newKeyInserted = true;
                }
                // this is the breaking up of the currNonLeafPage's keys,
                // which is the first key left in the current non-leaf page,
                // i.e. the first key that cannot be inserted in the new non-leaf node.
                // However, in the non-leaf node pushup, we don't need to
                // copy the element needed to push up in our current non-leaf page.
                // Therefore, we will not store it in the current non-leaf page.
                // Instead, we will directly push up and insert this key into the upper level parent non-leaf page.
                else if(newNonLeafPage -> slotTaken == threshold){
                    // push up this key to break up the new non-leaf Node and the current non-leaf node.
                    pushup = *((int *)key);
                    // Even though we don't copy or store the value of this key in any non-leaf node in this level, we cannot
                    // lose the pageId that this key is corresponding to.
                    // We will store the pageId corresponding to this pushup key at the end of the new non-leaf node.
                    if(fromLeaf == false){
                        newNonLeafPage -> pageNoArray[newNonLeafPage -> slotTaken] = leftPageId;
                    }
                    else{
                        PageId trueLeftPageId = currNonLeafPage -> pageNoArray[i];
                        currNonLeafPage -> pageNoArray[i] = leftPageId;
                        newNonLeafPage -> pageNoArray[newNonLeafPage -> slotTaken] = trueLeftPageId;
                    }
                    newKeyInserted = true;
                }
                // this is the case where we have to insert this new key
                // into the current non-leaf page
                else{
                    // shift down the slots in this current non-leaf page
                    // in this case, there are  currNonLeafPage -> slotTaken + 1 amount of slots having been moved away from this current non-leaf page already.
                     currNonLeafPage -> keyArray[i - newNonLeafPage -> slotTaken - 1] =  *((int*)key);
                    if(fromLeaf == false){
                     currNonLeafPage -> pageNoArray[i - newNonLeafPage -> slotTaken - 1] =  leftPageId;
                    }
                    else{
                        PageId trueLeftPageId = currNonLeafPage -> pageNoArray[i];
                        currNonLeafPage -> pageNoArray[i] = leftPageId;
                        currNonLeafPage -> pageNoArray[i - newNonLeafPage -> slotTaken - 1] =  trueLeftPageId;
                    }
                     currNonLeafPage -> slotTaken += 1;
                    newKeyInserted = true;
                }
            }
            // this is the part under when the new key has been
            // inserted already
            // now, we have to re-position the original i-th slot of the current non-leaf node
            if(newNonLeafPage -> slotTaken < threshold){
                newNonLeafPage -> keyArray[i+1] =  currNonLeafPage -> keyArray[i];
                newNonLeafPage -> pageNoArray[i+1] =  currNonLeafPage -> pageNoArray[i];
                newNonLeafPage -> slotTaken += 1;
                 currNonLeafPage -> slotTaken -= 1;
            }
            // this is the breaking up of the currNonLeafPage's keys,
            // which is the first key left in the current non-leaf page,
            // i.e. the first key that cannot be inserted in the new non-leaf node.
            // However, in the non-leaf node pushup, we don't need to
            // copy the element needed to push up in our current non-leaf page.
            // Therefore, we will not store it in the current non-leaf page.
            // Instead, we will directly push up and insert this key into the upper level parent non-leaf page.
            else if(newNonLeafPage -> slotTaken == threshold){
                // push up this key to break up the new non-leaf Node and the current non-leaf node.
                pushup =  currNonLeafPage -> keyArray[i];
                // Even though we don't copy or store the value of this key in any non-leaf node in this level, we cannot
                // lose the pageId that this key is corresponding to.
                // We will store the pageId corresponding to this pushup key at the end of the new non-leaf node.
                newNonLeafPage -> pageNoArray[newNonLeafPage -> slotTaken] =  currNonLeafPage -> pageNoArray[i];
                 currNonLeafPage -> slotTaken -= 1;
            }
            else{
                // shift down the slots in this current non-leaf page
                // It is clear that there are exact newNonLeafPage -> slotTaken amount of slots being removed
                // away from this current non-leaf page.
                 currNonLeafPage -> keyArray[i - newNonLeafPage -> slotTaken] =   currNonLeafPage -> keyArray[i];
                 currNonLeafPage -> pageNoArray[i - newNonLeafPage -> slotTaken] =   currNonLeafPage -> pageNoArray[i];
            }
        }
    }
    
    // it is remarked that during the for loop, we are only
    // shifting the pair of (leftPageId, key) around the current non-leaf node.
    // However, the original pageId at the end of the current non-leaf node has not been checked yet.
    // Now, we should move the original pageId at the end of the current non-leaf node, index at nodeOccupancy, to the correct position,
    // which should be the end of the updated pageNoArray of this current non-leaf node, at index  currNonLeafPage -> slotTaken.
     currNonLeafPage -> pageNoArray[currNonLeafPage -> slotTaken] =  currNonLeafPage -> pageNoArray[this -> nodeOccupancy];
    
    // another speical case is when all the keys in current non-leaf node are smaller than the new key.
    // Then, based on the current code, we have not inserted the new key yet.
    if(newKeyInserted == false){
        
        // DEBUG ONLY
        if(currNonLeafPage -> slotTaken + newNonLeafPage -> slotTaken != this -> nodeOccupancy)
            std::cout << "the specail case of all keys smaller than new is wrong! the new key is inserted already!" << std::endl;
        
        currNonLeafPage -> pageNoArray[currNonLeafPage -> slotTaken] = *((int *) key);
        if(fromLeaf == false){
            // the pageId currently at the end of the list should be on the right of this key
            PageId trueRightPage = currNonLeafPage -> pageNoArray[ currNonLeafPage -> slotTaken];
            currNonLeafPage -> pageNoArray[ currNonLeafPage -> slotTaken] = leftPageId;
            currNonLeafPage -> pageNoArray[ currNonLeafPage -> slotTaken + 1] = trueRightPage;
        }
        else{
            currNonLeafPage -> pageNoArray[ currNonLeafPage -> slotTaken + 1] = leftPageId;
        }
        newKeyInserted = true;
        currNonLeafPage -> slotTaken += 1;
    }
    
    // DEBUG ONLY:
    // to check wether some key and pageId pairs are missing
    if(currNonLeafPage -> slotTaken + newNonLeafPage -> slotTaken != this -> nodeOccupancy + 1)
        std::cout << "the final amount of inserting key into non-leaf and split is wrong!!!! And current total amount is: " << currNonLeafPage -> slotTaken + newNonLeafPage -> slotTaken << " , but expected to be: "<< this -> nodeOccupancy + 1 << std::endl;
    // check whether the new key has been inserted
    if(newKeyInserted == false)
        std::cout << "the new key has not been inserted yet!!!!" << std::endl;
    
    this -> bufMgr -> unPinPage(this -> file, pid, true);
    this -> bufMgr -> unPinPage(this -> file, newPageId, true);
    
    // check whether this current page is actually a root
    if(searchPath.size() == 0){
        // case when this current page is a root. Then, we need to
        // create a new non-leaf root.
        this -> createAndInsertNewRoot(&pushup, newPageId, pid, 0);
        // since this is a non-leaf node, then the nodes above this one
        // must be at level = 0 for sure.
    }
    else{
        // case when this current page is not a root.
        // get the upper level non-leaf parent node
        PageId parentId = searchPath[searchPath.size() - 1];
        // delete the parentId from the searchPath, to generate the search path for the parentId
        searchPath.erase(searchPath.begin() + searchPath.size() - 1);
        this -> insertNonLeafNode(parentId, &pushup, newPageId, searchPath, false);
    }
}

/**
 * Recursively find the page potentially containing the target key, which is the page id of the first element larger than or equal to the lower bound given.
 * @param key: a pointer to the value of the key that we are looking for
 * @param pid: the variable to return with, which contains the PageId of the
 * potentital target page.
 * @param currentPageId: the pageId of the current page we are at
 * @param searchPath: a vector of PageId contains all the PageId of the pages we have
 *  visited along our search path. The purpose of this vector is to benefit our insert later.
 *  It is remarked that the last leaf page id is not in this searchPath.
 */
const void BTreeIndex::searchLeafPageWithKey(const void *key, PageId & pid, PageId currentPageId, std::vector<PageId> & searchPath){
    Page * currPage;
    this -> bufMgr -> readPage(this -> file, currentPageId, currPage);
    NonLeafNodeInt * currNode = (NonLeafNodeInt *) currPage;

    int targetIndex = 0;
    int slotAvailable = currNode -> slotTaken;
    
    while(targetIndex < slotAvailable){
        if(*((int *) key) < currNode -> keyArray[targetIndex]){
            break;
        }
        else{
            targetIndex++;
        }
    }
    PageId updateCurrPageNum = currNode -> pageNoArray[targetIndex];
    // check if the next lower level node is leaf node or not
    if(currNode -> level == 1){
        this -> bufMgr -> unPinPage(this -> file, currentPageId, false);
        // write this current page Id into the search path
        searchPath.push_back(currentPageId);
        // this is the case where the next level is leaf node
        // we should just terminate the recurssion now
        pid = updateCurrPageNum;
    }
    else{
        // case where there is still non-leaf node in the next level
        this -> bufMgr -> unPinPage(this -> file, currentPageId, false);
        searchPath.push_back(currentPageId);
        // we keep on doing the recursion
        this -> searchLeafPageWithKey(key, pid, updateCurrPageNum, searchPath);
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------
/**
 *
 * This method is used to begin a filtered scan¡± of the index.
 *
 * For example, if the method is called using arguments (1,GT,100,LTE), then
 * the scan should seek all entries greater than 1 and less than or equal to
 * 100.
 *
 * @param lowValParm The low value to be tested.
 * @param lowOpParm The operation to be used in testing the low range.
 * @param highValParm The high value to be tested.
 * @param highOpParm The operation to be used in testing the high range.
 */
const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
    /*
    // DEBUG ONLY
    // check whether the non-leaf node structure are correct
    // we will search leaf page and slot location in leaf for all possible keys ranging from 0 to 4999
    int upperbound = 5000;
    for(int i = 0; i < upperbound; ++i){
        PageId testPid; // the page potentially containing the lowVal we want to find
        std::vector<PageId> testSearchPath; // the vector containing the path along searching
        
        // traverse from the root to find the first satisfied page
        // remember to unpin while traverse
        //check if the root is actually a leaf node
        
        this -> searchLeafPageWithKey(&i, testPid, this -> rootPageNum, testSearchPath);
        Page * testPage;
        this -> bufMgr -> readPage(this -> file, testPid, testPage);
        LeafNodeInt * testLeafNode = (LeafNodeInt*) testPage;
        
        int location = -1;
        
        // find the correct initial value for the nextEntry
        for(int j = 0; j < testLeafNode -> slotTaken; j++){
            // based on the assumpetion that we will only consider the case
            // of key as interger
            if(i == (int)testLeafNode -> keyArray[j]){
                location = j;
                break;
            }
        }
     
        if(location == -1){
            std::cout << "The non-leaf page structure is wrong! Cannot find the page path toward the page containing key: "<< i << std::endl;
        }
        this -> bufMgr -> unPinPage(this -> file, testPid, false);
    }
    std::cout << "All key has been searched!" << std::endl;
    */
    
    if(lowOpParm != GT && lowOpParm != GTE){
        throw BadOpcodesException();
    }
    if(highOpParm != LT && highOpParm != LTE){
        throw BadOpcodesException();
    }
    // If another scan is already executing, that needs to be ended here.
    if(this -> scanExecuting == true){
        this -> endScan();
    }

    // Set up all the variables for scan.
    switch (this -> attributeType) {
        case INTEGER:
            this -> lowValInt = *((int*) lowValParm);
            this -> highValInt = *((int*) highValParm);
            // check the validness of lowValParm and highValParm
            if(this -> lowValInt > this -> highValInt){
                throw BadScanrangeException();
            }
            break;
        case DOUBLE:
            this -> lowValDouble = *((double*) lowValParm);
            this -> highValDouble = *((double*) highValParm);
            // check the validness of lowValParm and highValParm
            if(this -> lowValDouble > this -> highValDouble){
                throw BadScanrangeException();
            }
            break;
        case STRING:
            // not sure how to handle the string case
            this -> lowValString = *((char*) lowValParm);
            this -> highValString = *((char*) highValParm);
            if(this -> lowValString > this -> highValString){
                throw BadScanrangeException();
            }
            break;
    }
    
    this -> scanExecuting = true;
    this -> lowOp = lowOpParm;
    this -> highOp = highOpParm;
    
    PageId pid; // the page potentially containing the lowVal we want to find
    std::vector<PageId> searchPath; // the vector containing the path along searching
    
    // traverse from the root to find the first satisfied page
    // remember to unpin while traverse
    //check if the root is actually a leaf node
    if (this -> rootIsLeaf){
        // if the root is already a leaf node, then it must be the only leaf node.
        pid = this -> rootPageNum;
    }
    else{
        this -> searchLeafPageWithKey(lowValParm, pid, this -> rootPageNum, searchPath);
    }
    
    // update the currentPageNum & currentPageData & nextEntry
    this -> currentPageNum = pid;
    this -> bufMgr -> readPage(this -> file, this -> currentPageNum, this -> currentPageData);
    LeafNodeInt * leafNode = (LeafNodeInt*) currentPageData;
    /*
    // DEBUG ONLY
    std::cout << "page search pid : "<< pid << " , slots taken: " << leafNode -> slotTaken << " , first few keys: " << leafNode -> keyArray[0] << " , second elem: " << leafNode -> keyArray[1] << std::endl;
    */
    this -> nextEntry = -1;
    
    // find the correct initial value for the nextEntry
    for(int i = 0; i < leafNode -> slotTaken; i++){
        // based on the assumpetion that we will only consider the case
        // of key as interger
        if(lowValInt >= leafNode -> keyArray[i]){
            if(lowOp == GTE && lowValInt == leafNode -> keyArray[i]){
                this -> nextEntry = i;
                break;
            }
        }
        else{
            // this is the case where key value at slot i is strict greater than the required low value bound.
            // we should stop the loop at the first such slot i happening.
            this -> nextEntry = i;
            break;
        }
    }
    // this is the case where there is no valid key entry to satisfy
    // this required lower bound of key, in our potential containing leaf page.
    if(this -> nextEntry == -1){
        // throw error if none satisfied page exist, and call endScan before throwing
        this -> bufMgr -> unPinPage(this -> file, this -> currentPageNum, false);
        endScan();
        throw NoSuchKeyFoundException();
    }
    // this is the case where a valid key entry, which is greater than or equal to the lower bound of key, is found.
    // now we need to check whether this valid key entry has a valid record id and whether this valid key entry satisfies the condition under the upper bound of key.
    
    RecordId curRid = leafNode -> ridArray[nextEntry];
    if ((curRid.page_number == 0 && curRid.slot_number == 0) ||
         leafNode -> keyArray[nextEntry] > highValInt ||
        (leafNode -> keyArray[nextEntry] == highValInt && highOp == LT))
        {
          // throw error if none satisfied page exist, and call endScan before throwing
          this -> bufMgr -> unPinPage(this -> file, this -> currentPageNum, false);
          endScan();
          throw NoSuchKeyFoundException();
        }
    this -> bufMgr -> unPinPage(this -> file, this -> currentPageNum, false);
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------
/**
 * This method fetches the record id of the next tuple that matches the scan
 * criteria. If the scan has reached the end, then it should throw the
 * following exception: IndexScanCompletedException.
 *
 * For instance, if there are two data entries that need to be returned in a
 * scan, then the third call to scanNext must throw
 * IndexScanCompletedException. A leaf page that has been read into the buffer
 * pool for the purpose of scanning, should not be unpinned from buffer pool
 * unless all records from it are read or the scan has reached its end. Use
 * the right sibling page number value from the current leaf to move on to the
 * next leaf which holds successive key values for the scan.
 *
 * @param outRid An output value;This is the record id of the next entry
 *                that matches the scan filter set in startScan.
 */
const void BTreeIndex::scanNext(RecordId& outRid) 
{
    // throws ScanNotInitializedException
    // If no scan has been initialized.
    if(this -> scanExecuting == false){
        throw ScanNotInitializedException();
    }
    this -> bufMgr -> readPage(this -> file, this -> currentPageNum, this -> currentPageData);
    // If no more records, satisfying the scan criteria, are left to be scanned.
    // we use the nextEntry == -2 to represent that there is no more
    // satisfied records later. I.e. the scanning is complete.
    if(this -> nextEntry == -2){
        // since the scaning is complete, we should
        // unpin this current page.
        
        //DEBUG ONLY
        //std::cout << "scan is done with page num: " <<this -> currentPageNum << std::endl;
        
         // there is issue happening if we repeatedly unpin a page
         // the catch seems not working. Or there is different seg fault
         // being thrown out.
        try{
            this -> bufMgr -> unPinPage(this -> file, this -> currentPageNum, false);
        }
        catch(PageNotPinnedException e){
        }
        
        //std::cout << "unpin with page num is done as scan complete: " <<this -> currentPageNum << std::endl;
        throw IndexScanCompletedException();
    }
    // Fetch the record id of the next index entry that matches the scan.
    LeafNodeInt * currPage = (LeafNodeInt *) (this -> currentPageData);
    // Return the next record from current page being scanned.
    
     // DEBUG ONLY
    /*
    std::cout << "key value associate with the rid: " << currPage -> keyArray[this -> nextEntry] << std::endl;
    std::cout << "entry value: " << this -> nextEntry << std::endl;
    std::cout << "slot amount taken: " << currPage -> slotTaken << std::endl;
    */
     
    outRid = currPage -> ridArray[this -> nextEntry];
    // move the scanner to the next satisfied record
    // check whether theer is more records in this current leaf node or not
    if(this -> nextEntry < currPage  -> slotTaken - 1){
        // this is the case where theere are still other valid key and
        // rids in this current index page
        // check whether the next valid record in the current index
        // page is still satisfied
        switch(this -> highOp){
            case LT:
                if(currPage  -> keyArray[this -> nextEntry + 1] < highValInt){
                    this -> nextEntry += 1;
                    try{
                        this -> bufMgr -> unPinPage(this -> file, this -> currentPageNum, false);
                    }
                    catch(PageNotPinnedException e){
                    }
                    return;
                }
                else{
                    // this case is where there is no more satisifed
                    // entry existing. I.e. the scan is complete
                    
                    //std::cout << "this is the last page to scan with pageId(1): " << this -> currentPageNum<<std::endl;
                    try{
                        this -> bufMgr -> unPinPage(this -> file, this -> currentPageNum, false);
                    }
                    catch(PageNotPinnedException e){
                    }
                    this -> nextEntry = -2;
                    return;
                }
                break;
            case LTE:
                if(currPage  -> keyArray[this -> nextEntry + 1] <= highValInt){
                    this -> nextEntry += 1;
                    try{
                        this -> bufMgr -> unPinPage(this -> file, this -> currentPageNum, false);
                    }
                    catch(PageNotPinnedException e){
                    }
                    return;
                }
                else{
                    // this case is where there is no more satisifed
                    // entry existing. I.e. the scan is complete
                    
                    //std::cout << "this is the last page to scan with pageId(2): " << this -> currentPageNum<<std::endl;
                    this -> nextEntry = -2;
                    try{
                        this -> bufMgr -> unPinPage(this -> file, this -> currentPageNum, false);
                    }
                    catch(PageNotPinnedException e){
                    }
                    return;
                }
                break;
            default:
                break;
        }
    }
    else{
         // this is the case of reaching the end of current index page:
        // prepare to move to scan the next leaf page:
        // case where there is no more next right sib page
        if(currPage  -> rightSibPageNo == Page::INVALID_NUMBER){
            // this case is where there is no more satisifed
            // entry existing. I.e. the scan is complete
            
            //DEBUG ONLY
            //std::cout << "stop at here! no more right sib!" << std::endl;
            //std::cout << "this is the last page to scan with pageId(3): " << this -> currentPageNum<<std::endl;
            
            this -> nextEntry = -2;
            // this is the case of reaching the end of current index page:
            // unpin this current page
            this -> bufMgr -> unPinPage(this -> file, this -> currentPageNum, false);
            return;
        }
        else{
            // move to the next index page
            PageId nextPage = currPage -> rightSibPageNo;
            // unpin this current page
            this -> bufMgr -> unPinPage(this -> file, this -> currentPageNum, false);
            // update the var for the current scanned page to the next page
            this -> currentPageNum = nextPage;
            this -> bufMgr -> readPage(this -> file, this -> currentPageNum, this -> currentPageData);
            // Note that we will not unpin this new scanned leaf index
            // page in this method. Because the user will call the
            // endScan method, if the scan is complete. Therefore,
            // we will leave for the endScan method to unpin this new
            // scanned page.
            currPage = (LeafNodeInt *) this -> currentPageData;
            // check whether the first slot in the new index page has
            // valid record or not. I.e. check whether this first slot
            // has been taken up or not
            if(currPage -> slotTaken == 0){
                // this is the case where the first slot has not been
                // taken yet. Then this leaf index page has not been
                // taken with any records yet.
                
                // DEBUG ONLY
                //std::cout << "stop at here! no new records in the new page!" << std::endl;
                //std::cout << "this is the last page to scan with pageId(4): " << this -> currentPageNum<<std::endl;
                
                this -> nextEntry = -2;
                try{
                    this -> bufMgr -> unPinPage(this -> file, this -> currentPageNum, false);
                }
                catch(PageNotPinnedException e){
                }
                return;
            }
            // check whether the first record in the new index page is still satisfied
            switch(this -> highOp){
                case LT:
                    if(currPage -> keyArray[0] < highValInt){
                        this -> nextEntry = 0;
                    }
                    else{
                        // this case is where there is no more satisifed
                        // entry existing. I.e. the scan is complete
                        
                        // DEBUG ONLY
                        //std::cout << "current vlaue in key array " << currPage -> keyArray[0]  << std::endl;
                        //std::cout << "current slot amount in this array " << currPage -> slotTaken  << std::endl;
                         //std::cout << "this is the last page to scan with pageId(5): " << this -> currentPageNum<<std::endl;
                        
                        this -> nextEntry = -2;
                    }
                    break;
                case LTE:
                    if(currPage -> keyArray[0] <= highValInt){
                        this -> nextEntry = 0;
                    }
                    else{
                        // this case is where there is no more satisifed
                        // entry existing. I.e. the scan is complete
                        
                        // DEBUG ONLY
                        //std::cout << "current vlaue in key array " << currPage -> keyArray[0]  << std::endl;
                        //std::cout << "current slot amount in this array " << currPage -> slotTaken  << std::endl;
                         //std::cout << "this is the last page to scan with pageId(6): " << this -> currentPageNum<<std::endl;
                        
                        this -> nextEntry = -2;
                    }
                    break;
                default:
                    break;
            }
            try{
                this -> bufMgr -> unPinPage(this -> file, this -> currentPageNum, false);
            }
            catch(PageNotPinnedException e){
            }
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
/**
 * This method terminates the current scan and unpins all the pages that have
 * been pinned for the purpose of the scan.
 * It throws ScanNotInitializedException when called before a successful
 * startScan call.
 */
const void BTreeIndex::endScan() 
{
    std::cout << "Get into the endScan now! " << std::endl;
    
    // the case where there is no scan being initialized.
    if(this -> scanExecuting == false){
        throw ScanNotInitializedException();
    }
    
    // recover all the values for the variables of scanning
    switch(this -> attributeType){
        case INTEGER:
            // recover the attribute into some invalid number
            this -> lowValInt = -1;
            this -> highValInt = -1;
            break;
        case DOUBLE:
            this -> lowValDouble = -1;
            this -> highValDouble = -1;
            break;
        case STRING:
            this -> lowValString = "";
            this -> highValString = "";
            break;
    }
    this -> scanExecuting = false;
    this -> nextEntry = -1;
    this -> currentPageNum = Page::INVALID_NUMBER;
    this -> currentPageData = NULL;
    this -> lowOp = (Operator)-1;
    this -> highOp = (Operator)-1;
    
    std::cout << "Get out of  the endScan now! " << std::endl;
}


}
