/*
 * buffer_mgr.c
 *
 *  Created on: 2013-4-29
 *      Author: aminy
 */


/*
 * buffer_mgr.c
 *
 *  Created on: 2013-3-31
 *      Author: aminy
 */


/*
 ============================================================================
 Name        : 525Assignment2.c
 Author      : YANJIA XU, LUWEI ZHANG, SHUTING YIN
 Version     :
 Copyright   : Your copyright notice
 Description : implement the methods of buffer_mgr.h
 ============================================================================
 */

#include "buffer_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <malloc.h>
#include <time.h>


#define RC_PAGE_IS_USING -2
#define RC_CANNOT_FORCE_PAGE -3



#define LEN sizeof(struct BCB)


FILE *fp;
int buffersize;
int n;
SM_FileHandle fh;
int readCount=0;
int writeCount=0;

//define a struct, use this struct to handle every frame on bufferpool
struct BCB{
	int frameNum;
	int pageNum;
	int fixCount;
	int dirty;
	int timeCount;
	struct BCB * next;
}BCB;

struct BCB *head;
struct BCB *p1,*p2;

//create link
struct BCB *create(void)
{

	n=0;
	p1=p2=(struct BCB *)malloc(LEN);


	p1->frameNum=0;
	p1->pageNum=-1;
	p1->fixCount=0;
	p1->dirty=FALSE;
	p1->timeCount=0;

	head=NULL;
	while(p1->frameNum<buffersize)
	{
		n=n+1;
		if(n==1)
		{
			head=p1;
		}
		else{
			p2->next=p1;
		}
		p2=p1;

		p1=(struct BCB *)malloc(LEN);
		p1->frameNum=p2->frameNum+1;
		p1->pageNum=-1;
		p1->fixCount=0;
		p1->dirty=FALSE;
		p1->timeCount=0;
	}
	p2->next=NULL;

	return(head);

}


// Buffer Manager Interface Pool Handling
//initial bufferpool
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData)
{
//	bm->pageFile=pageFileName;
	bm->numPages=numPages;
	bm->strategy=strategy;
	bm->mgmtData=stratData;

	buffersize=bm->numPages;

	head=create();
	openPageFile(bm->pageFile,&fh);


	return RC_OK;

}




//strategy FIFO

int rs_FIFO(struct BCB *temp)
{
	int fid;
//	fid=(pageNum)%buffersize;
	int min=temp->timeCount;
	while(temp!=NULL)
	{
		if(min>temp->timeCount && temp->fixCount==0)
		{
			break;
		}
		temp=temp->next;
	}

	if(temp!=NULL)
	{
		//min=temp->timeCount;
		fid=temp->frameNum;
		temp->timeCount++;
	}
	else
	{
		temp=head;
		while(temp!=NULL)
		{
			if(temp->fixCount==0)
			{
				break;
			}
			temp=temp->next;
		}
		if(temp!=NULL)
		{
			fid=temp->frameNum;
			temp->timeCount++;
		}
		else
		{
			printf("PAGE IS IN USE");
		}
	}


	return fid;
}

//strategy LRU
int rs_LRU(struct BCB *temp,const PageNumber pageNum)
{
	int fid=-1;
	fid=(pageNum)%(buffersize);

	while(temp!=NULL)
	{
		if(temp->frameNum==fid)
		{
			break;
		}
		temp=temp->next;
	}
	if(temp!=NULL)
	{
		if(temp->fixCount==0)
		{
			fid=(pageNum)%(buffersize);
		}
		else
		{
			printf("PAGE IS IN USE");
		}
	}

	return fid;
}




void readblock(BM_PageHandle *const page,
        const PageNumber pageNum)
{
	if(fh.totalNumPages<=pageNum)
	{
		appendEmptyBlock(&fh);

	}

	char data[PAGE_SIZE]={"Page-"};
	char str[PAGE_SIZE];
	itoa(page->pageNum,str,10);
	strcat(data,str);

	page->data=data;
	readCount++;
}
void writeblock()
{
	writeCount++;

}


// Buffer Manager Interface Access Pages
/**
 * pin a page, first check if this page has been in bufferpool, if it exists, return frame id to client
 * otherwise check if there is free frame in the bufferpool, if exists, load this page to the free frame
 * othterwise if bufferpool is full, use one strategy to select a frame and  replace that frame
 */
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum)
{
	int fid=-1;
	int freeFrame=FALSE;


    struct BCB *temp;

	temp=head;


	//check if page has been in the buffer cache

	while(temp!=NULL)
	{
		if(temp->pageNum==pageNum)
		{
			break;
		}
		temp=temp->next;
	}


	if(temp!=NULL)                         //page has been in one frame of bufferpool
	{
		fid=temp->frameNum;
		page->pageNum=pageNum;
		temp->fixCount++;
	//	temp->timeCount++;
	//	readblock(page,pageNum);
		return RC_OK;
	}
	else                                 //page not in the buffer
	{
		//find if exist free frame
		temp=head;
		while(temp!=NULL)
		{
			if(temp->pageNum==-1)
			{
				freeFrame=TRUE;
				break;
			}
			temp=temp->next;

		}

		//if there is free frame

		if(freeFrame==TRUE)
		{
			fid=temp->frameNum;
			temp->pageNum=pageNum;
			temp->fixCount++;
			page->pageNum=pageNum;
			readblock(page,pageNum);

			return RC_OK;

		}
		//no free frame, use strategy to replace
		else{
			temp=head;

			switch (bm->strategy)
		    {

		    case RS_FIFO:                      //use FIFO strategy
		      fid=rs_FIFO(temp);

		      while(temp!=NULL)
		      {
		    	  if(temp->frameNum==fid)
		    	  {
		    		  break;
		    	  }
		    	  temp=temp->next;
		      }

		      //can replace now
		      if(temp!=NULL)
		      {
		    	  if(temp->dirty==TRUE)
		    	  {
		    		  writeblock();
		    	  }

		    	  temp->pageNum=pageNum;
		          temp->fixCount++;

		          page->pageNum=pageNum;
		          readblock(page,pageNum);
		      }
		      break;
		    case RS_LRU:                      //use LRU strategy
		      fid=rs_LRU(temp,pageNum);
		      while(temp!=NULL)
		     	 {
		     		  if(temp->frameNum==fid)
		     		    {
		     		    	break;
		     		     }
		     		   temp=temp->next;
		     	}

		     	//can replace now
		     	if(temp!=NULL)
		     	{
		     		if(temp->dirty==TRUE)
		     		 {
		     		     writeblock();
		     		   }

		     	 temp->pageNum=pageNum;
		     	 temp->fixCount++;

		     	page->pageNum=pageNum;
		     	readblock(page,pageNum);
		     	}
		      break;
		    default:
		      printf("%i", bm->strategy);
		      break;
		    }

		return RC_OK;
		}

	}

}

//make the page dirty
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	struct BCB *temp;
	temp=head;

	while(temp!=NULL)
	{
		if(temp->pageNum==page->pageNum)
		{
			break;
		}
		temp=temp->next;
	}
	temp->dirty=TRUE;
	return RC_OK;
}

//unpin a page
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{

	struct BCB *temp;
	temp=head;
	while(temp!=NULL)
	{
		if(temp->pageNum==page->pageNum)
		{
			break;
		}
		temp=temp->next;
	}
	temp->fixCount--;
	return RC_OK;
}
//force a page, write page back to disk
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	struct BCB *temp;
	temp=head;

	while(temp!=NULL)
	{
		if(temp->pageNum==page->pageNum)
		{
			break;
		}
		temp=temp->next;
	}
	if(temp!=NULL)
	{
		//only dirty page can be forced into disk, and fixCount do not change
		if(temp->dirty==TRUE)
		{
			char data[PAGE_SIZE]={0};
			page->data=data;
			return RC_OK;
		}
		else
		{
			return RC_CANNOT_FORCE_PAGE;
		}
	}
	else return NO_PAGE;

}

//shut down a buffer pool
RC shutdownBufferPool(BM_BufferPool *const bm)
{
	struct BCB *temp;
	temp=head;
	while(temp!=NULL)
	{
		if(temp->fixCount!=0)
		{
			break;
		}
		temp=temp->next;
	}
	if(temp!=NULL)
	{
		return RC_PAGE_IS_USING;
	}
	else
	{
		temp=head;
		while(temp!=NULL)
		{
			if(temp->dirty==TRUE)
			{
				temp->dirty=FALSE;
			}
			temp=temp->next;
		}


	readCount=0;
	writeCount=0;
	temp=NULL;
	head=NULL;
	bm->pageFile=NULL;
	bm->mgmtData=NULL;
	bm->numPages=0;
//	fclose(fp);
//	remove(bm->pageFile);

	free(p1);
	free(p2);
	return RC_OK;
    }
}
//all drity page from buffer pool should be writen back to disk
RC forceFlushPool(BM_BufferPool *const bm)
{
	struct BCB *temp;
	temp=head;
	while(temp!=NULL)
	{
		if(temp->dirty==TRUE)
		{
			if(temp->fixCount==0)
			{
				temp->dirty=FALSE;
				writeblock();
			}
			else{
				printf("PAGE IS IN USE");
			}

		}
		temp=temp->next;
	}


	return RC_OK;
}




// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	struct BCB *temp;
	temp=head;
	buffersize=bm->numPages;
	PageNumber *pagenumber=(int *)malloc(sizeof(int)*buffersize);
	//int * a = ;
	int i;
	for(i=0;i<bm->numPages;i++)
	{
		while(temp!=NULL)
		{
			if(temp->frameNum==i)
			{
				break;
			}
			temp=temp->next;
		}
		if(temp!=NULL)
		{
			if(temp->pageNum==-1)
			{
				pagenumber[i]=NO_PAGE;
			}
			else
			{
				pagenumber[i]=temp->pageNum;
			}
		}
	}


	return pagenumber;
}
bool *getDirtyFlags (BM_BufferPool *const bm)
{
	struct BCB *temp;
	temp=head;
	bool *dirtyflags=(bool *)malloc(sizeof(bool)*buffersize);
	int i;
	for(i=0;i<bm->numPages;i++)
	{
		while(temp!=NULL)
		{
			if(temp->frameNum==i)
			{
				break;
			}
			temp=temp->next;
		}
		if(temp!=NULL)
		{
			dirtyflags[i]=temp->dirty;
		}
	}

	return dirtyflags;
}
int *getFixCounts (BM_BufferPool *const bm)
{
	struct BCB *temp;
	temp=head;
	buffersize=bm->numPages;
	int *fixcounts=(int *)malloc(sizeof(int)*buffersize);
	int i;
	for(i=0;i<bm->numPages;i++)
	{
		while(temp!=NULL)
		{
			if(temp->frameNum==i)
			{
				break;
			}
			temp=temp->next;
		}
		if(temp!=NULL)
		{
			fixcounts[i]=temp->fixCount;
		}
	}


	return fixcounts;
}
int getNumReadIO (BM_BufferPool *const bm)
{

	return readCount;
}
int getNumWriteIO (BM_BufferPool *const bm)
{

	return writeCount;
}

