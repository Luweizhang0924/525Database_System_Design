

/*
 * btree_mgr.c
 *
 *  Created on: 2013-4-22
 *      Author: Luwei Zhang
 */

#include "btree_mgr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "record_mgr.h"
#include "storage_mgr.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>


typedef struct Btree{
	struct Value val;
	struct RID rid;
}Btree;

int entry;
int nextentry;
Btree **btree;
// init and shutdown index manager
RC initIndexManager (void *mgmtData)
{

	btree=(Btree **)malloc(100*sizeof(Btree* ));


	return RC_OK;
}
RC shutdownIndexManager ()
{
	int i;
	for(i=0;i<entry;i++)
	{
		free(btree[entry]);
	}
	free(btree);

	return RC_OK;
}

// create, destroy, open, and close an btree index
RC createBtree (char *idxId, DataType keyType, int n)
{
	BTreeHandle *tree;
	tree=(BTreeHandle *)malloc(sizeof(BTreeHandle)*3);
	tree->idxId=idxId;
	tree->keyType=keyType;
	entry=0;
	nextentry=0;

	return RC_OK;
}
RC openBtree (BTreeHandle **tree, char *idxId)
{
	*tree=(BTreeHandle *)malloc(sizeof(tree)*3);
	(*tree)->idxId=(char *)malloc(sizeof(char)*4);
	(*tree)->idxId=idxId;

	return RC_OK;
}
RC closeBtree (BTreeHandle *tree)
{
	free(tree);


	return RC_OK;
}
RC deleteBtree (char *idxId)
{
	free(idxId);
	entry=0;
	nextentry=0;

	return RC_OK;
}

// access information about a b-tree
RC getNumNodes (BTreeHandle *tree, int *result)
{

	int node;
	int i,j;
	int count=0;
	for(i=1;i<entry;i++)
	{
		for(j=i-1;j>=0;j--)
		{
			if(btree[i]->rid.page==btree[j]->rid.page)
			{
				count++;
				break;
			}
		}

	}
	node=entry-count;


	*result=node;
	return RC_OK;
}
RC getNumEntries (BTreeHandle *tree, int *result)
{
	*result=entry;
	return RC_OK;
}
RC getKeyType (BTreeHandle *tree, DataType *result)
{
	int i;
	for(i=0;i<entry;i++)
	{
		result[i]=tree->keyType;
	}

	return RC_OK;
}

// index access
RC findKey (BTreeHandle *tree, Value *key, RID *result)
{
	int i;
	bool find=FALSE;
	switch(key->dt)
	{
	case DT_INT:
		for(i=0;i<entry;i++)
	    {
		    if(btree[i]->val.dt==key->dt && btree[i]->val.v.intV==key->v.intV)
		   {
			   find=TRUE;
			   break;
		   }
	    }
		break;
	case DT_STRING:
		for(i=0;i<entry;i++)
		{
			if(btree[i]->val.dt==key->dt && (strcmp(btree[i]->val.v.stringV,key->v.stringV)==0))
			{
				find=TRUE;
				break;
			}
		}
		break;
	case DT_FLOAT:
		for(i=0;i<entry;i++)
		{
			if(btree[i]->val.dt==key->dt && btree[i]->val.v.floatV==key->v.floatV)
			{
				find=TRUE;
				break;
			}
		}
		break;
	case DT_BOOL:
		for(i=0;i<entry;i++)
		{
			if(btree[i]->val.dt==key->dt && btree[i]->val.v.boolV==key->v.boolV)
			{
				find=TRUE;
				break;
			}
		}
		break;
	}

	if(find==TRUE)
	{
		result->page=btree[i]->rid.page;
		result->slot=btree[i]->rid.slot;
		return RC_OK;
	}
	else{
		return RC_IM_KEY_NOT_FOUND;
	}


}
RC insertKey (BTreeHandle *tree, Value *key, RID rid)
{
	btree[entry]=(Btree *)malloc(sizeof(Btree));
	int i;
	bool find=FALSE;
	//first check if this key already exists
	switch(key->dt)
	{
	case DT_INT:
		for(i=0;i<entry;i++)
	    {
		    if(btree[i]->val.dt==key->dt && btree[i]->val.v.intV==key->v.intV)
		   {
			   find=TRUE;
			   break;
		   }
	    }
		break;
	case DT_STRING:
		for(i=0;i<entry;i++)
		{
			if(btree[i]->val.dt==key->dt && (strcmp(btree[i]->val.v.stringV,key->v.stringV)==0))
			{
				find=TRUE;
				break;
			}
		}
		break;
	case DT_FLOAT:
		for(i=0;i<entry;i++)
		{
			if(btree[i]->val.dt==key->dt && btree[i]->val.v.floatV==key->v.floatV)
			{
				find=TRUE;
				break;
			}
		}
		break;
	case DT_BOOL:
		for(i=0;i<entry;i++)
		{
			if(btree[i]->val.dt==key->dt && btree[i]->val.v.boolV==key->v.boolV)
			{
				find=TRUE;
				break;
			}
		}
		break;
	}

	if(find==TRUE)
	{
		return RC_IM_KEY_ALREADY_EXISTS;
	}
	else
	{
		switch(key->dt)
		{
		case DT_INT:
			btree[entry]->val.dt=key->dt;
			btree[entry]->val.v.intV=key->v.intV;
			break;
		case DT_STRING:
			btree[entry]->val.dt=key->dt;
			strcpy(btree[entry]->val.v.stringV,key->v.stringV);
			break;
		case DT_FLOAT:
			btree[entry]->val.dt=key->dt;
			btree[entry]->val.v.floatV=key->v.floatV;
			break;
		case DT_BOOL:
			btree[entry]->val.dt=key->dt;
			btree[entry]->val.v.boolV=key->v.boolV;
			break;
		}
		btree[entry]->rid.page=rid.page;
		btree[entry]->rid.slot=rid.slot;
		entry++;

		return RC_OK;
	}


}
RC deleteKey (BTreeHandle *tree, Value *key)
{
	int i,j;
	int temp=0;

	bool find=FALSE;

	//first check if this key exists
	switch(key->dt)
	{
	case DT_INT:
		for(i=0;i<entry;i++)
	    {
		    if(btree[i]->val.dt==key->dt && btree[i]->val.v.intV==key->v.intV)
		   {
			   find=TRUE;
			   break;
		   }
	    }
		break;
	case DT_STRING:
		for(i=0;i<entry;i++)
		{
			if(btree[i]->val.dt==key->dt && (strcmp(btree[i]->val.v.stringV,key->v.stringV)==0))
			{
				find=TRUE;
				break;
			}
		}
		break;
	case DT_FLOAT:
		for(i=0;i<entry;i++)
		{
			if(btree[i]->val.dt==key->dt && btree[i]->val.v.floatV==key->v.floatV)
			{
				find=TRUE;
				break;
			}
		}
		break;
	case DT_BOOL:
		for(i=0;i<entry;i++)
		{
			if(btree[i]->val.dt==key->dt && btree[i]->val.v.boolV==key->v.boolV)
			{
				find=TRUE;
				break;
			}
		}
		break;
	}
	if(find==TRUE)
	{
		temp=i+1;

		for(j=i;j<entry&&temp<entry;j++)
		{
			switch(btree[temp]->val.dt)
			{
			case DT_INT:
				btree[j]->val.dt=btree[temp]->val.dt;
				btree[j]->val.v.intV=btree[temp]->val.v.intV;
				break;
			case DT_STRING:
				btree[j]->val.dt=btree[temp]->val.dt;
				strcpy(btree[j]->val.v.stringV,btree[temp]->val.v.stringV);
				break;
			case DT_FLOAT:
				btree[j]->val.dt=btree[temp]->val.dt;
				btree[j]->val.v.floatV=btree[temp]->val.v.floatV;
				break;
			case DT_BOOL:
				btree[j]->val.dt=btree[temp]->val.dt;
				btree[j]->val.v.boolV=btree[temp]->val.v.boolV;
				break;
			}
			btree[j]->rid.page=btree[temp]->rid.page;
			btree[j]->rid.slot=btree[temp]->rid.slot;
			temp++;
		}
		entry--;
		free(btree[j]);
	    return RC_OK;
	}
	else
	{
		return RC_IM_KEY_NOT_FOUND;
	}

}
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
//	handle->tree=tree;


	return RC_OK;
}
void swap(Btree **btree,int a,int b)
{
	Value valtemp;
	RID ridtemp;

	valtemp.dt=btree[a]->val.dt;
	valtemp.v.intV=btree[a]->val.v.intV;
	ridtemp.page=btree[a]->rid.page;
	ridtemp.slot=btree[a]->rid.slot;

	btree[a]->val.dt=btree[b]->val.dt;
	btree[a]->val.v.intV=btree[b]->val.v.intV;
	btree[a]->rid.page=btree[b]->rid.page;
	btree[a]->rid.slot=btree[b]->rid.slot;

	btree[b]->val.dt=valtemp.dt;
	btree[b]->val.v.intV=valtemp.v.intV;
	btree[b]->rid.page=ridtemp.page;
	btree[b]->rid.slot=ridtemp.slot;


}
RC nextEntry (BT_ScanHandle *handle, RID *result)
{
	//sort
	int i,k;
	for(i=0;i<entry-1;i++)
	{
		int pos=i;
		for(k=i+1;k<entry;k++)
		{
			if(btree[k]->val.v.intV<btree[pos]->val.v.intV)
			{
				pos=k;
			}
		}
		swap(btree,i,pos);

	}

	if(nextentry<entry)
	{
		result->page=btree[nextentry]->rid.page;
		result->slot=btree[nextentry]->rid.slot;
		nextentry++;
		return RC_OK;
	}
	else
	{
		return RC_IM_NO_MORE_ENTRIES;
	}

}
RC closeTreeScan (BT_ScanHandle *handle)
{
	free(handle);

	return RC_OK;
}
/*
// debug and test functions
char *printTree (BTreeHandle *tree)
{

	return tree->idxId;
}
*/
