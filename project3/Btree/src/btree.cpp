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
    
    this -> scanExecuting = false;
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
    FileScan fileScan(relationName, bufMgrIn);
    try
    {
        RecordId scanRid;
        while(1)
        {
            fileScan.scanNext(scanRid);
            std::string recordStr = fileScan.getRecord();
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
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
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

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}

}
