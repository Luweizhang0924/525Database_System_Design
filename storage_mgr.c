/*
 * storage_mgr.c
 *
 *  Created on: 2013-4-29
 *      Author: aminy
 */





#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"



/**************************************************
 *                   interface                    *
 **************************************************/
/*manipulating page files*/



FILE *fp;

//initial storage manager
void initStorageManager(void){

}

//create a new page file, single page and zero bytes
RC createPageFile(char *fileName){
    FILE *file;
	file = fopen(fileName, "w+b");                     //create a file
	if (file==NULL)
		return RC_FILE_NOT_FOUND;


	char page[PAGE_SIZE]={0};
	fwrite(page,PAGE_SIZE,1,file);


	fclose(file);

	return RC_OK;

}

//open an existing page file
RC openPageFile(char *fileName, SM_FileHandle *fHandle){
	long fileLen;                              //the total length of the file(the total bytes of the file)

    fp=fopen(fileName,"r+b");                      //open the file

	if(fp==NULL)
	{
		return RC_FILE_NOT_FOUND;
	}

	fHandle->fileName=fileName;                  //get file name
	fHandle->curPagePos=0;                       //get current position
	//get total pages
	fseek(fp,0L,SEEK_END);                            //move pointer to point the end of the file
	fileLen=ftell(fp);                                //calculate the total bytes of this file
	rewind(fp);                                       //move pointer to point the beginning of this file
	fHandle->totalNumPages=fileLen/PAGE_SIZE;         //calculate total number pages


	return RC_OK;

}

//close an open page
RC closePageFile(SM_FileHandle *fHandle){

	fHandle->curPagePos=-1;
	fHandle->totalNumPages=-1;
	return RC_OK;
}

//destroy a page file
RC destroyPageFile(char *fileName){
	fclose(fp);
	remove(fileName);
	return RC_OK;
}

/* reading blocks from disc */

//read the pageNumth block from a file and stores its content in the memory
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){

	if(fHandle->totalNumPages<pageNum)
		{
			return RC_READ_NON_EXISTING_PAGE;
		}
	//rewind(fp);

	fHandle->curPagePos=pageNum;
	fseek(fp,pageNum*PAGE_SIZE,0);                      //move the pointer to point the first byte of pageNumth block
	int r =fread(memPage,PAGE_SIZE,1,fp);                    //read the pageNumth block and store content into memory

	if(r!=1)                                               //check if this page can be read
	{
		if(feof(fp)){fclose(fp);}
		printf("file cannot read\n");
	}


	return RC_OK;

}

//return the current page position of a file
int getBlockPos (SM_FileHandle *fHandle){
	return fHandle->curPagePos;
}

//read the first page of a file
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	rewind(fp);                                                      //move pointer to the beginning
	int r= fread(memPage,PAGE_SIZE,1,fp);                        //read first block into memPage

	if(r!=1)                                               //check if this page can be read
		{
			if(feof(fp)){fclose(fp);}
			printf("file cannot read\n");
		}

	fHandle->curPagePos=0;                                       //make sure the curPagePos is in the first page
	return RC_OK;
}

//read the current, previous, and next page relative to the curPagePos of the file
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){

	int curPos=fHandle->curPagePos;
	fseek(fp,((curPos-1)*PAGE_SIZE),0);                 //move pointer to point the beginning of previous page
	int r= fread(memPage,PAGE_SIZE,1,fp);

	if(r!=1)                                               //check if this page can be read
			{
				if(feof(fp)){fclose(fp);}
				printf("file cannot read\n");
			}
	fHandle->curPagePos=curPos-1;                       //move curPagePos to the page that was read
	return RC_OK;
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	fseek(fp,((fHandle->curPagePos)*PAGE_SIZE),0);           //move pointer to point the beginning of current page
	int r= fread(memPage,PAGE_SIZE,1,fp);
	if(r!=1)                                               //check if this page can be read
				{
					if(feof(fp)){fclose(fp);}
					printf("file cannot read\n");
				}
	return RC_OK;
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	int curPos=fHandle->curPagePos;
	fseek(fp,((fHandle->curPagePos+1)*PAGE_SIZE),0);         //move pointer to point the beginning of next page
	int r=fread(memPage,PAGE_SIZE,1,fp);
	if(r!=1)                                               //check if this page can be read
				{
					if(feof(fp)){fclose(fp);}
					printf("file cannot read\n");
				}
	fHandle->curPagePos=curPos+1;                         //move curPagePos to the page that was read
	return RC_OK;
}

//read the last page of the file
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	fseek(fp,(fHandle->totalNumPages-1)*PAGE_SIZE, 0);                     //point to the last block
	int r=fread(memPage,PAGE_SIZE,1,fp);                                         //read the last block
	if(r!=1)                                               //check if this page can be read
					{
						if(feof(fp)){fclose(fp);}
						printf("file cannot read\n");
					}
	fHandle->curPagePos=fHandle->totalNumPages;                            //move curPagePos to the last page
	return RC_OK;

}

/* writing blocks to a page file */

//write a page to disk
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){


	if (fHandle->totalNumPages < pageNum)
	{
		return RC_READ_NON_EXISTING_PAGE;
	}

	fseek(fp, pageNum*PAGE_SIZE, 0);           //move pointer to point the pageNumth page
	int w=fwrite(memPage,PAGE_SIZE,1,fp);       //write content of memPage into block
	if(w!=1)                                    //check if block can be written
	{
		printf("file write error\n");
	}

	return RC_OK;

}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	fseek(fp,(fHandle->curPagePos*PAGE_SIZE),0);                    //move pointer to point the current page
   int w= fwrite(memPage,PAGE_SIZE,1,fp);                                 //write content of memPage into block
	if(w!=1)                                    //check if block can be written
	{
		printf("file write error\n");
	}

	return RC_OK;
}

//increase the number of pages in the file by one
RC appendEmptyBlock (SM_FileHandle *fHandle){
	fseek(fp,(fHandle->totalNumPages*PAGE_SIZE),0);            //move pointer to point the beginning of the appending block
	char newblock[PAGE_SIZE] = {0};                           //the new last page should be filled with zero bytes
	int w = fwrite(newblock, PAGE_SIZE,1,fp);                            //write new last page
	if(w!=1)                                    //check if block can be written
		{
			printf("file write error\n");
		}
	//update fHandle
	fHandle->curPagePos=fHandle->totalNumPages+1;                //move curPagePos to the last new block
	fHandle->totalNumPages=fHandle->totalNumPages+1;             //increase total number pages
	return RC_OK;
}

//increase the size to numberOfPages
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){

	if(fHandle->totalNumPages<numberOfPages)
	{
		//fseek(fp,(fHandle->totalNumPages*PAGE_SIZE),0);
		fHandle->totalNumPages=numberOfPages;                          //increase total number pages to numberOfPages
	}

	return RC_OK;
}

/*
void printError(RC error) {

}

*/


