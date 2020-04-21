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

//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

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
        BlobFile file =  BlobFile::open(indexName);
        // check whether the existing file matching with our input info
        // find the meta page
        PageId metaPageId = file.getFirstPageNo();
        Page * metaPage;
        this -> bufMgr -> readPage(&file, metaPageId, metaPage);
        IndexMetaInfo * metaInfo = (IndexMetaInfo *) metaPage;
        // check the attribute in the meta page
        if(!(metaInfo -> relationName == relationName
           && metaInfo -> attrByteOffset == attrByteOffset
           && metaInfo -> attrType == attrType)){
            this -> bufMgr -> unPinPage(&file, metaPageId, false);
            throw BadIndexInfoException("Index file exists but values in metapage not match.");
        }
        this -> headerPageNum = metaPageId;
        this -> rootPageNum = metaInfo -> rootPageNo;
        this -> file = (&file);
        this -> bufMgr -> unPinPage(&file, metaPageId, false);
        return;
    }
    
    BlobFile file = BlobFile::create(indexName);
    this -> file = & file;
    // create the metadata page
    PageId metaPageId;
    Page * metaPage;
    this -> bufMgr -> allocPage(&file, metaPageId, metaPage);
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
    this -> bufMgr -> allocPage(&file, rootPageId, rootPage);
    this -> bufMgr -> unPinPage(&file, rootPageId, true);
    metaInfo -> rootPageNo = rootPageId;
    this -> rootPageNum = rootPageId;
    this -> bufMgr -> unPinPage(&file, metaPageId, true);
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

BTreeIndex::~BTreeIndex()
{
    // end all scans
    try{
        endScan();
    }
    catch(ScanNotInitializedException e){
    }
    // unpin all the pages from the index file, i.e. BlobFile
    // Flush this index file
    // the unpin process can be guaranteed by the endScan method already
    this -> bufMgr -> flushFile(this -> file);
    // delete the index file
    delete this -> file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
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
    // traverse from the root to find the first satisfied page
    // remember to unpin while traverse
    // update the currentPageNum & currentPageData & nextEntry
    // throw error if none satisfied page exist, and call endScan before throwing
    // TODO
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
    // throws ScanNotInitializedException
    // If no scan has been initialized.
    if(this -> scanExecuting == false){
        throw ScanNotInitializedException();
    }
    // If no more records, satisfying the scan criteria, are left to be scanned.
    // we use the nextEntry == -2 to represent that there is no more
    // satisfied records later. I.e. the scanning is complete.
    if(this -> nextEntry == -2){
        // since the scaning is complete, we should
        // unpin this current page.
        try{
            this -> bufMgr -> unPinPage(this -> file, this -> currentPageNum, false);
        }
        catch(PageNotPinnedException e){
        }
        throw IndexScanCompletedException();
    }
    // Fetch the record id of the next index entry that matches the scan.
    LeafNodeInt currPage = *((LeafNodeInt *) this -> currentPageData);
    // Return the next record from current page being scanned.
    outRid = currPage.ridArray[this -> nextEntry];
    // move the scanner to the next satisfied record
    // check whether current index page is reaching the end or not
    if(this -> nextEntry < this -> leafOccupancy - 1){
        // check whether there is more records in this leaf node:
        // this is the case where the key value in the next slot is
        // valid, then it means there is next valid record in the slot
        if(currPage.keyArray[this -> nextEntry + 1] >= 0){
            // check whether the next valid record in the current index
            // page is still satisfied
            switch(this -> highOp){
                case LT:
                    if(currPage.keyArray[this -> nextEntry + 1] < highValInt){
                        this -> nextEntry += 1;
                    }
                    else{
                        // this case is where there is no more satisifed
                        // entry existing. I.e. the scan is complete
                        this -> nextEntry = -2;
                        return;
                    }
                    break;
                case LTE:
                    if(currPage.keyArray[this -> nextEntry + 1] <= highValInt){
                        this -> nextEntry += 1;
                    }
                    else{
                        // this case is where there is no more satisifed
                        // entry existing. I.e. the scan is complete
                        this -> nextEntry = -2;
                        return;
                    }
                    break;
                default:
                    break;
            }
        }
        // this is the case where the key value in the next slot is
        // invalid, then it means there is no more valid records in this
        // current index page. We should move on to the next index page
    }
    // this is the case of reaching the end of current index page for sure:
    // unpin this current page
    this -> bufMgr -> unPinPage(this -> file, this -> currentPageNum, false);
    // prepare to move to scan the next leaf page:
    // case where there is no more next right sib page
    if(currPage.rightSibPageNo == Page::INVALID_NUMBER){
        // this case is where there is no more satisifed
        // entry existing. I.e. the scan is complete
        this -> nextEntry = -2;
        return;
    }
    else{
        // move to the next index page
        this -> currentPageNum = currPage.rightSibPageNo;
        this -> bufMgr -> readPage(this -> file, this -> currentPageNum, this -> currentPageData);
        currPage = *((LeafNodeInt *) this -> currentPageData);
        // check whether the first slot in the new index page has valid
        // record or not
        if(currPage.keyArray[0] < 0){
            // this is the case where the key value is invalid.
            // it means that there is no more valid record existing.
            this -> nextEntry = -2;
            return;
        }
        // check whether the first record in the new index page is still satisfied
        switch(this -> highOp){
            case LT:
                if(currPage.keyArray[0] < highValInt){
                    this -> nextEntry = 0;
                }
                else{
                    // this case is where there is no more satisifed
                    // entry existing. I.e. the scan is complete
                    this -> nextEntry = -2;
                }
                break;
            case LTE:
                if(currPage.keyArray[0] <= highValInt){
                    this -> nextEntry = 0;
                }
                else{
                    // this case is where there is no more satisifed
                    // entry existing. I.e. the scan is complete
                    this -> nextEntry = -2;
                }
                break;
            default:
                break;
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
    // the case where there is no scan being initialized.
    if(this -> scanExecuting == false){
        throw ScanNotInitializedException();
    }
    // unpin the current page
    try{
        this -> bufMgr -> unPinPage(this -> file, this -> currentPageNum, false);
    }
    catch(PageNotPinnedException e){
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
}

}
