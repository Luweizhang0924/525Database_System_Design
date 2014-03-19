/*
 * record_mgr.c
 *
 *  Created on: 2013-4-29
 *      Author: aminy
 */




#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include "record_mgr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"



/**
 * transfer from int  to strings
 */
void myitoa(int num,char *str,int radix)
{
	char index[]="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	unsigned unum;
	int i=0,j,k;

	if(radix==10&&num<0)
	{
		unum=(unsigned)-num;
		str[i++]='-';
	}
	else
	{
		unum=(unsigned)num;
	}

	do{
		str[i++]=index[unum%(unsigned)radix];
		unum/=radix;
	}while(unum);

	str[i]='\0';

	if(str[0]=='-')
		{k=1;}
	else {k=0;}

	char temp;
	for(j=k;j<=(i-k-1)/2;j++)
	{
		temp=str[j];
		str[j]=str[i-j-1];
		str[i-j-1]=temp;
	}
}


int tupleNum;
int pageNum;
char **rec;
int nextrecord;
BM_BufferPool *bm;
BM_PageHandle *h;

#define PAGENAME "hello.bin"

// table and manager
RC initRecordManager (void *mgmtData)
{

	pageNum=0;


	return RC_OK;
}
RC shutdownRecordManager ()
{
	shutdownBufferPool(bm);
	free(bm);
	free(h);
	return RC_OK;
}
RC createTable (char *name, Schema *schema)
{
	//create a table should create the underlying page file
	//the record manager should access the pages of the file through the bufer manager
	createPageFile(PAGENAME);

	bm=MAKE_POOL();
	h=MAKE_PAGE_HANDLE();
	initBufferPool(bm, PAGENAME, 3, RS_FIFO, NULL);

	rec=(char **)malloc(100*sizeof(char *));
	RM_TableData *table = (RM_TableData *) malloc(PAGE_SIZE);
	table->name=name;
	table->schema=schema;
	tupleNum=0;
	nextrecord=0;
	h->pageNum=pageNum;

	pageNum++;

	return RC_OK;
}
//open a table
RC openTable (RM_TableData *rel, char *name)
{
	rel->name=name;
	pinPage(bm,h,h->pageNum);

	return RC_OK;
}
RC closeTable (RM_TableData *rel)
{
	rel->name=NULL;

	return RC_OK;
}
RC deleteTable (char *name)
{
	name=NULL;
	int i=0;
	for(i=0;i<tupleNum;i++)
	{
		free(rec[tupleNum]);
	}
	free(rec);

	tupleNum=0;
	nextrecord=0;

	unpinPage(bm,h);

	pageNum--;

    return RC_OK;
}
int getNumTuples (RM_TableData *rel)
{

	return tupleNum;
}

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record)
{

	record->id.slot=tupleNum;
	record->id.page=pageNum-1;

	rec[tupleNum]=(char *)malloc(4);
	strcpy(rec[tupleNum],record->data);

	tupleNum++;

	return RC_OK;
}
RC deleteRecord (RM_TableData *rel, RID id)
{
	rec[id.slot]=NULL;
	return RC_OK;
}
RC updateRecord (RM_TableData *rel, Record *record)
{

	strcpy(rec[record->id.slot],record->data);

	return RC_OK;
}
RC getRecord (RM_TableData *rel, RID id, Record *record)
{
	record->id.page=id.page;
	record->id.slot=id.slot;

	strcpy(record->data,rec[id.slot]);

	return RC_OK;
}

// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
	scan->rel=rel;
	return RC_OK;
}
RC next (RM_ScanHandle *scan, Record *record)
{
	if(nextrecord<tupleNum)
	{
		record->id.slot=nextrecord;

		strcpy(record->data,rec[nextrecord]);
	    nextrecord++;
	    return RC_OK;
	}
	else
	{
		return RC_RM_NO_MORE_TUPLES;
	}



}
RC closeScan (RM_ScanHandle *scan)
{
	if(scan->rel!=NULL)
		free(scan->rel);
	free(scan);
	return RC_OK;
}

// dealing with schemas
int getRecordSize (Schema *schema)
{
	int size=0;
	int i=0;
	for(i=tupleNum-1;i>=0;i--)
	{
		if(rec[i]!=NULL)
		{
			size=strlen(rec[i]);
			break;
		}
	}

	return size;
}
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
	Schema *schema;
	schema=(Schema *)malloc(sizeof(Schema));
	schema->attrNames=attrNames;
	schema->numAttr=numAttr;
	schema->dataTypes=dataTypes;
	schema->typeLength=typeLength;
	schema->keyAttrs=keys;
	schema->keySize=keySize;

	return schema;

}
RC freeSchema (Schema *schema)
{
	free(schema->attrNames);
	free(schema->dataTypes);
	free(schema->keyAttrs);
	free(schema->typeLength);
	schema->keySize=-1;
	schema->numAttr=-1;
	free(schema);

	return RC_OK;
}

// dealing with records and attribute values

RC createRecord (Record **record, Schema *schema)
{
	*record=(Record *)malloc(sizeof(Record));
	//initialize allocate enough memory to the data
	int size=0;
	int i=0;
	for(i=0;i<schema->numAttr;i++)
	{
		switch(schema->dataTypes[i])
		{
		case DT_INT:
			size+=sizeof(int);
			break;
		case DT_STRING:
			size+=schema->typeLength[i];
			break;
		case DT_FLOAT:
			size+=sizeof(float);
			break;
		case DT_BOOL:
			size+=sizeof(bool);
			break;
		}
	}


	(*record)->data=(char *)malloc(size);

	return RC_OK;
}

RC freeRecord (Record *record)
{

	free(record->data);
	free(record);

	return RC_OK;
}
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
	*value=(Value *)malloc(sizeof(Value));
	char *c=(char *)malloc(4);

	int *pos=(int *)malloc(sizeof(int));
	int i=0;
	int num=0;

	for(i=0;i<strlen(record->data);i++)
	{
		if(record->data[i]==',')
		{
			pos[num]=i;
			num++;
			//break;
		}
	}


	if(attrNum==0)
	{
		switch(schema->dataTypes[attrNum])
		{
		case DT_INT:
			(*value)->dt=schema->dataTypes[attrNum];
			strncpy(c,record->data,pos[attrNum]);
			(*value)->v.intV=atoi(c);
			break;
		case DT_STRING:
			(*value)->dt=schema->dataTypes[attrNum];
			(*value)->v.stringV=(char *)malloc(5);
			strncpy(c,record->data,pos[attrNum]);
			c[pos[attrNum]]='\0';
			strcpy((*value)->v.stringV,c);
			break;
		case DT_FLOAT:
			(*value)->dt=schema->dataTypes[attrNum];
			strncpy(c,record->data,pos[attrNum]);
			(*value)->v.floatV=atof(c);
			break;
		case DT_BOOL:
			(*value)->dt=schema->dataTypes[attrNum];
			strncpy(c,record->data,pos[attrNum]);
			if(strcmp(c,"TRUE")==0)
			{
				(*value)->v.boolV=TRUE;
			}
			else
			{
				(*value)->v.boolV=FALSE;
			}
			break;
		}
	}


	if(attrNum>0&&attrNum<num)
	{
		switch(schema->dataTypes[attrNum])
	    {

	    case DT_INT:
	    	(*value)->dt=schema->dataTypes[attrNum];
	    	strncpy(c,record->data+pos[attrNum-1]+1,pos[attrNum]-pos[attrNum-1]-1);
	    	(*value)->v.intV=atoi(c);
	    	break;
	    case DT_STRING:
	    	(*value)->dt=schema->dataTypes[attrNum];
	    	(*value)->v.stringV=(char *)malloc(5);
	    	strncpy(c,record->data+pos[attrNum-1]+1,pos[attrNum]-pos[attrNum-1]-1);
	    	c[pos[attrNum]-pos[attrNum-1]-1]='\0';
	    	strcpy((*value)->v.stringV,c);
	    	break;
	    case DT_FLOAT:
	    	(*value)->dt=schema->dataTypes[attrNum];
	    	strncpy(c,record->data+pos[attrNum-1]+1,pos[attrNum]-pos[attrNum-1]-1);
	    	(*value)->v.floatV=atof(c);
		    break;
	    case DT_BOOL:
	    	(*value)->dt=schema->dataTypes[attrNum];
	    	strncpy(c,record->data+pos[attrNum-1]+1,pos[attrNum]-pos[attrNum-1]-1);
	    	if(strcmp(c,"TRUE")==0)
	    	{
	    		(*value)->v.boolV=TRUE;
	    	}
	    		else
	    	{
	    		(*value)->v.boolV=FALSE;
	    	}
		    break;
	}
	}
	if(attrNum==num)
	{

		switch(schema->dataTypes[attrNum])
	    {

	    case DT_INT:
	    	(*value)->dt=schema->dataTypes[attrNum];
	    	strncpy(c,record->data+pos[attrNum-1]+1,strlen(record->data)-1-pos[attrNum-1]);
	    	(*value)->v.intV=atoi(c);
	    	break;
	    case DT_STRING:
	    	(*value)->dt=schema->dataTypes[attrNum];
	    	(*value)->v.stringV=(char *)malloc(5);
	    	strncpy(c,record->data+pos[attrNum-1]+1,strlen(record->data)-1-pos[attrNum-1]);
	    	c[strlen(record->data)-1-pos[attrNum-1]]='\0';
	    	strcpy((*value)->v.stringV,c);
	    	break;
	    case DT_FLOAT:
	    	(*value)->dt=schema->dataTypes[attrNum];
	    	strncpy(c,record->data+pos[attrNum-1]+1,strlen(record->data)-1-pos[attrNum-1]);
	    	(*value)->v.floatV=atof(c);
		    break;
	    case DT_BOOL:
	    	(*value)->dt=schema->dataTypes[attrNum];
	    	strncpy(c,record->data+pos[attrNum-1]+1,strlen(record->data)-1-pos[attrNum-1]);
	    	if(strcmp(c,"TRUE")==0)
	    	{
	    		(*value)->v.boolV=TRUE;
	    	}
	    	else
	    	{
	    		(*value)->v.boolV=FALSE;
	    	}
		    break;
	}
	}

	return RC_OK;
}
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
	char *c=(char *)malloc(4);


	int *pos=(int *)malloc(sizeof(int));
	int i=0;
	int num=0;

	for(i=0;i<strlen(record->data);i++)
	{
		if(record->data[i]==',')
		{
			pos[num]=i;
			num++;
			//break;
		}
	}

	if(num<schema->numAttr-1)
	{
	if(attrNum==0)
	{
		switch(schema->dataTypes[attrNum])
		{
		case DT_INT:
			myitoa(value->v.intV,record->data,10);
			break;
		case DT_STRING:
			record->data=value->v.stringV;
			break;
		case DT_FLOAT:
			myitoa(value->v.floatV,record->data,10);
			break;
		case DT_BOOL:
			if(value->v.boolV==TRUE)
			{
				record->data="TRUE";
			}
			else
			{
				record->data="FALSE";
			}
			break;
		}
	}
	else{
		switch(schema->dataTypes[attrNum])
		{

		case DT_INT:
			strcat(record->data,",");
			myitoa(value->v.intV,c,10);
			strcat(record->data,c);
			break;
		case DT_STRING:
			strcat(record->data,",");
			strcat(record->data,value->v.stringV);
			break;
		case DT_FLOAT:
			strcat(record->data,",");
			myitoa(value->v.floatV,c,10);
			strcat(record->data,c);
			break;
		case DT_BOOL:
			strcat(record->data,",");
			if(value->v.boolV==TRUE)
			{
				c="TRUE";
			}
			else
			{
				c="FALSE";
			}
			strcat(record->data,c);
			break;
		}
	}
	}
	else
	{
		if(attrNum==0)
		{
			switch(schema->dataTypes[attrNum])
			{
			case DT_INT:
				myitoa(value->v.intV,c,10);
				strncpy(record->data,c,pos[attrNum]);
				break;
			case DT_STRING:
				strncpy(record->data,value->v.stringV,pos[attrNum]);
				break;
			case DT_FLOAT:
				myitoa(value->v.floatV,c,10);
				strncpy(record->data,c,pos[attrNum]);
				break;
			case DT_BOOL:
				if(value->v.boolV==TRUE)
				{
					c="TRUE";
				}
				else
				{
					c="FALSE";
				}
				strncpy(record->data,c,pos[attrNum]);
				break;
			}
		}

		if(attrNum>0&&attrNum<num)
		{
			switch(schema->dataTypes[attrNum])
		    {

		    case DT_INT:
		    	myitoa(value->v.intV,c,10);
		    	strncpy(record->data+pos[attrNum-1]+1,c,pos[attrNum]-pos[attrNum-1]-1);
		    	break;
		    case DT_STRING:
		    	strncpy(record->data+pos[attrNum-1]+1,value->v.stringV,pos[attrNum]-pos[attrNum-1]-1);
		    	break;
		    case DT_FLOAT:
		    	myitoa(value->v.floatV,c,10);
		    	strncpy(record->data+pos[attrNum-1]+1,c,pos[attrNum]-pos[attrNum-1]-1);
			    break;
		    case DT_BOOL:
		    	if(value->v.boolV==TRUE)
		    	{
		    		c="TRUE";
		    	}
		    	else
		    	{
		    		c="FALSE";
		    	}
		    	strncpy(record->data+pos[attrNum-1]+1,c,pos[attrNum]-pos[attrNum-1]-1);
			    break;
		}
		}
		if(attrNum==num)
		{

			switch(schema->dataTypes[attrNum])
		    {

		    case DT_INT:
		    	myitoa(value->v.intV,c,10);
		    	strncpy(record->data+pos[attrNum-1]+1,c,strlen(record->data)-1-pos[attrNum-1]);
		    	break;
		    case DT_STRING:
		    	strncpy(record->data+pos[attrNum-1]+1,value->v.stringV,strlen(record->data)-1-pos[attrNum-1]);
		    	break;
		    case DT_FLOAT:
		    	myitoa(value->v.floatV,c,10);
		    	strncpy(record->data+pos[attrNum-1]+1,c,strlen(record->data)-1-pos[attrNum-1]);
			    break;
		    case DT_BOOL:
		    	if(value->v.boolV==TRUE)
		    	{
		    		c="TRUE";
		    	}
		    	else
		    	{
		    		c="FALSE";
		    	}
		    	strncpy(record->data+pos[attrNum-1]+1,c,strlen(record->data)-1-pos[attrNum-1]);
			    break;
		}
		}

	}
	return RC_OK;
}
