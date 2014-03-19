/*
 * buffer_mgr_stat.h
 *
 *  Created on: 2013-4-29
 *      Author: aminy
 */

#ifndef BUFFER_MGR_STAT_H
#define BUFFER_MGR_STAT_H

#include "buffer_mgr.h"

// debug functions
void printPoolContent (BM_BufferPool *const bm);
void printPageContent (BM_PageHandle *const page);
char *sprintPoolContent (BM_BufferPool *const bm);
char *sprintPageContent (BM_PageHandle *const page);

#endif
