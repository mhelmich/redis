/*
 * Copyright (c) 2009-2014, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2009-2014, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/*-----------------------------------------------------------------------------
 * Sorted skiplist API
 *----------------------------------------------------------------------------*/

/* This skiplist implementation is almost a C translation of the original
 * algorithm described by William Pugh in "Skip Lists: A Probabilistic
 * Alternative to Balanced Trees", modified in three ways:
 * a) this implementation allows for repeated scores.
 * b) the comparison is not just by key (our 'score') but by satellite data.
 * c) there is a back pointer, so it's a doubly linked list with the back
 * pointers being only at "level 0". This allows to traverse the list
 * from tail to head, useful for finding the first item with a score. */

/*
 * Optimization potential:
 * - build search function that returns pointer to node or splice position
 *   (insert and delete need to be able to work on this pointer)
 */

#include "redis.h"
#include <math.h>

/*-----------------------------------------------------------------------------
 * creating the objects
 *----------------------------------------------------------------------------*/

slNode *slCreateNode(int level, robj *score, robj *obj) {
    slNode *zn = zmalloc(sizeof(*zn) + level * sizeof(struct skiplistLevel));
    zn->score = score;
    zn->obj = obj;
    return zn;
}

skiplist *slCreate(void) {
    int j;
    skiplist *sl;

    sl = zmalloc(sizeof(*sl));
    sl->level = 1;
    sl->length = 0;
    sl->header = slCreateNode(SKIPLIST_MAXLEVEL, NULL, NULL );
    for (j = 0; j < SKIPLIST_MAXLEVEL; j++) {
        sl->header->level[j].forward = NULL;
    }
    sl->header->backward = NULL;
    sl->tail = NULL;
    return sl;
}

void slFreeNode(slNode *node) {
    decrRefCount(node->obj);
    decrRefCount(node->score);
    zfree(node);
}

void slFree(skiplist *sl) {
    slNode *node = sl->header->level[0].forward, *next;
    zfree(sl->header);
    while (node) {
        next = node->level[0].forward;
        slFreeNode(node);
        node = next;
    }
    zfree(sl);
}

/*-----------------------------------------------------------------------------
 * internal utils
 *----------------------------------------------------------------------------*/

/* Returns a random level for the new skiplist node we are going to create.
 * The return value of this function is between 1 and SKIPLIST_MAXLEVEL
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned. */
int slRandomLevel(void) {
    int level = 1;
    while ((random() & 0xFFFF) < (SKIPLIST_P * 0xFFFF))
        level += 1;
    return (level < SKIPLIST_MAXLEVEL) ? level : SKIPLIST_MAXLEVEL;
}

/* Internal function used by slDelete, slDeleteByScore and slDeleteByRank */
void slDeleteNode(skiplist *sl, slNode *x, slNode **update) {
    int i;
    for (i = 0; i < sl->level; i++) {
        if (update[i]->level[i].forward == x) {
            update[i]->level[i].forward = x->level[i].forward;
        }
    }
    if (x->level[0].forward) {
        x->level[0].forward->backward = x->backward;
    } else {
        sl->tail = x->backward;
    }
    while (sl->level > 1 && sl->header->level[sl->level - 1].forward == NULL )
        sl->level--;
    sl->length--;
}

/* Parse max or min argument of SLRANGE.
 * (foo means foo (open interval)
 * [foo means foo (closed interval)
 * - means the min string possible
 * + means the max string possible
 *
 * If the string is valid the *dest pointer is set to the redis object
 * that will be used for the comparision, and ex will be set to 0 or 1
 * respectively if the item is exclusive or inclusive. REDIS_OK will be
 * returned.
 *
 * If the string is not a valid range REDIS_ERR is returned, and the value
 * of *dest and *ex is undefined. */
int slParseRangeItem(robj *item, robj **dest, int *ex) {
    char *c = item->ptr;

    switch (c[0]) {
    case '+':
        if (c[1] != '\0') return REDIS_ERR;
        *ex = 0;
        *dest = shared.maxstring;
        incrRefCount(shared.maxstring);
        return REDIS_OK;
    case '-':
        if (c[1] != '\0') return REDIS_ERR;
        *ex = 0;
        *dest = shared.minstring;
        incrRefCount(shared.minstring);
        return REDIS_OK;
    case '(':
        *ex = 1;
        *dest = createStringObject(c + 1, sdslen(c) - 1);
        return REDIS_OK;
    case '[':
        *ex = 0;
        *dest = createStringObject(c + 1, sdslen(c) - 1);
        return REDIS_OK;
    default:
        *ex = 0;
        *dest = createStringObject(c, sdslen(c));
        return REDIS_OK;
    }
}

/* Populate the rangespec according to the objects min and max.
 *
 * Return REDIS_OK on success. On error REDIS_ERR is returned.
 * When OK is returned the structure must be freed with slFreeRange(),
 * otherwise no release is needed. */
int slParseRange(robj *min, robj *max, slrangespec *spec) {
    /* The range can't be valid if objects are integer encoded. */
    if (min->encoding == REDIS_ENCODING_INT
            || max->encoding == REDIS_ENCODING_INT) return REDIS_ERR;

    spec->min = spec->max = NULL;
    if (slParseRangeItem(min, &spec->min, &spec->minex) == REDIS_ERR
            || slParseRangeItem(max, &spec->max, &spec->maxex) == REDIS_ERR) {
        if (spec->min) decrRefCount(spec->min);
        if (spec->max) decrRefCount(spec->max);
        return REDIS_ERR;
    } else {
        return REDIS_OK;
    }
}

/*
 * compares two score objects
 * return values are as follows:
 * if *score1 < *score2 => >0
 * if *score1 = *score2 =>  0
 * if *score1 > *score2 => <0
 */
int slCmp(robj *score1, robj *score2) {
    if (score1 == NULL && score2 == NULL ) return 0;
    if (score1 == NULL && score2 != NULL ) return 1;
    if (score1 != NULL && score2 == NULL ) return -1;
    int cmp = 0;
    if (score1->type == REDIS_STRING && score2->type == REDIS_STRING) {
        if (score1->encoding == REDIS_ENCODING_INT
                && score2->encoding == REDIS_ENCODING_INT) {
            long score1Long = (long) score1->ptr;
            long score2Long = (long) score2->ptr;
            cmp = score1Long - score2Long;
        } else {
            cmp = compareStringObjects(score1, score2);
        }
    }
    return cmp;
}

/* Free a skiplist range structure, must be called only after slParseRange()
 * populated the structure with success (REDIS_OK returned). */
void slFreeRange(slrangespec *spec) {
    decrRefCount(spec->min);
    decrRefCount(spec->max);
}

/*-----------------------------------------------------------------------------
 * data structure methods
 *----------------------------------------------------------------------------*/

/*
 * Returns the first node in a search.
 * All the following nodes from here are in order and need to be iterated.
 * */
slNode *slSearchSmallestNode(skiplist *sl, robj *score) {
    slNode *x = sl->header;
    for (int i = sl->level - 1; i >= 0; i--) {
        while (x->level[i].forward) {
            int cmp = slCmp(x->level[i].forward->score, score);
            if (cmp < 0) {
                /* move pointer forward */
                x = x->level[i].forward;
            } else if (cmp == 0) {
                x = x->level[i].forward;
                /* backtrack to the first score */
                while (x->backward && slCmp(x->score, score) > 0) {
                    x = x->backward;
                }
                return x;
            } else {
                /* in any other condition we want to
                 * break out of the while loop */
                break;
            }
        }
    }
    return NULL ;
}

/* This function finds the low end for a range query.
 * It returns the first qualifying node or NULL if the min entry in the list
 * is smaller then the range max.
 * A node qualifies as suitable lower end for a range query iff:
 * - its score is at least equal the range minimum
 * - the score is non existent the next node is chosen
 * - the score exists in the list
 */
slNode *slRangeSmallestNode(skiplist *sl, slrangespec *range,
        int *foundexactvalue) {
    /* before we do any work we check whether the
     * range minimum is greater or equal the score of the first node */
    if (slCmp(sl->header->level[0].forward->score, range->max) > 0)
        return NULL ;

    slNode *x = sl->header;
    for (int i = sl->level - 1; i >= 0; i--) {
        while (x->level[i].forward) {
            int cmp = slCmp(x->level[i].forward->score, range->min);
            if (cmp < 0) {
                /* move pointer forward */
                x = x->level[i].forward;
            } else if (cmp == 0) {
                /* advance pointer and then start to backtrack if needed */
                x = x->level[i].forward;
                while (!range->minex && x->backward && x->backward != sl->header
                        && slCmp(x->backward->score, range->min) == 0) {
                    /* if we are supposed to exclude the min
                     * we need to keep searching in this case */
                    x = x->backward;
                }
                *foundexactvalue = 1;
                return x;
            } else if (i == 0 && cmp > 0) {
                *foundexactvalue = 0;
                return x->level[i].forward;
            } else {
                /* in any other condition we want to
                 * break out of the while loop */
                break;
            }
        }
    }
    return NULL ;
}

/*
 * This method guarantees to return the smallest relevant node in a range query.
 * Relevant here means that this method accommodates for excluded minimums.
 * This method might return NULL if the asked minimum is outside the range.
 * */
slNode *slRangeLowEnd(skiplist *sl, slrangespec *range) {
    int foundexactvalue;
    slNode *smallest = slRangeSmallestNode(sl, range, &foundexactvalue);
    while (range->minex && foundexactvalue
            && slCmp(smallest->score, range->min) == 0) {
        /* loop forward if the minimum is supposed to be excluded */
        if (!smallest->level[0].forward) {
            return NULL ;
        } else {
            smallest = smallest->level[0].forward;
        }
    }
    return smallest;
}

/* This function finds the high end for a range query.
 * It returns the first qualifying node or NULL if the max entry in the list
 * is smaller then the range min.
 * A node qualifies as suitable higher end for a range query iff:
 * - its score is at most equal the range minimum
 * - the score is non existent the next node is chosen
 * - the score is existent
 */
slNode *slRangeLargestNode(skiplist *sl, slrangespec *range, int *foundexactvalue) {
    if (slCmp(sl->tail->score, range->max) < 0) return sl->tail;

    slNode *x = sl->header;
    for (int i = sl->level - 1; i >= 0; i--) {
        while (x->level[i].forward) {
            int cmp = slCmp(x->level[i].forward->score, range->max);
            if (cmp < 0) {
                /* move pointer forward until we find the score */
                x = x->level[i].forward;
            } else if (range->maxex && cmp == 0) {
                /* if the maximum is supposed to be excluded
                 * and the score is found, short-circuit out
                 * since we have to backtrack anyway */
                *foundexactvalue = 1;
                return x->level[i].forward;
            } else if (!range->maxex && cmp == 0) {
                if (x->level[i].forward->level[i].forward
                        && slCmp(x->level[i].forward->level[i].forward->score, range->max) == 0) {
                    /* in this case we can fast forward on this level to a comparable score */
                    x = x->level[i].forward;
                } else if (i == 0
                        /* here is an implicit
                         * && slCmp(x->level[i].forward->level[i].forward->score, range->max) > 0 */) {
                    *foundexactvalue = 1;
                    return x->level[i].forward;
                } else {
                    /* if we can't advance on this level and
                     * if this is not the deepest level,
                     * the search continues one level down */
                    break;
                }
            } else if (cmp > 0 && i == 0) {
                /* the value doesn't exist hence we return the current forward node */
                *foundexactvalue = 0;
                return x->level[i].forward;
            } else {
                break;
            }
        }
    }

    return NULL ;
}

slNode *slRangeHighEnd(skiplist *sl, slrangespec *range) {
    int foundexactvalue = 0;
    slNode *largest = slRangeLargestNode(sl, range, &foundexactvalue);
    while (range->maxex && foundexactvalue
            && slCmp(largest->score, range->max) == 0) {
        if (!largest->backward) {
            return NULL ;
        } else {
            largest = largest->backward;
        }
    }
    return largest;
}

/**
 * TODO
 * clean this mess up!!!
 */
slNode *slInsert(skiplist *sl, robj *score, robj *obj) {
    slNode *update[SKIPLIST_MAXLEVEL], *x;
    int i, level;

    x = sl->header;
    for (i = sl->level - 1; i >= 0; i--) {
        while (x->level[i].forward) {
            int cmp = slCmp(x->level[i].forward->score, score);
            if (cmp < 0 || (cmp == 0 && slCmp(x->level[i].forward->obj, obj) < 0)) {
                x = x->level[i].forward;
            } else {
                break;
            }
        }
        update[i] = x;
    }

    level = slRandomLevel();
    if (level > sl->level) {
        for (i = sl->level; i < level; i++) {
            update[i] = sl->header;
        }
        sl->level = level;
    }

    x = slCreateNode(level, score, obj);
    for (i = 0; i < level; i++) {
        x->level[i].forward = update[i]->level[i].forward;
        update[i]->level[i].forward = x;
    }

    x->backward = (update[0] == sl->header) ? NULL : update[0];
    if (x->level[0].forward) {
        x->level[0].forward->backward = x;
    } else {
        sl->tail = x;
    }
    sl->length++;

    return x;
}

/* Delete an element with matching score/object from the skiplist. */
int slDelete(skiplist *sl, robj *score, robj *obj) {
    slNode *update[SKIPLIST_MAXLEVEL], *x;
    int i;

    x = sl->header;
    for (i = sl->level - 1; i >= 0; i--) {
        while (x->level[i].forward
                && (slCmp(x->level[i].forward->score, score) < 0
                || (slCmp(x->level[i].forward->score, score) == 0 && compareStringObjects(x->level[i].forward->obj, obj) < 0)))
            x = x->level[i].forward;
        update[i] = x;
    }
    /* We may have multiple elements with the same score, what we need
     * is to find the element with both the right score and object. */
    x = x->level[0].forward;
    if (x && slCmp(score, x->score) == 0 && equalStringObjects(x->obj, obj)) {
        slDeleteNode(sl, x, update);
        slFreeNode(x);
        return 1;
    }
    return 0; /* not found */
}

int slDeleteScore(skiplist *sl, robj *score) {
    slNode *update[SKIPLIST_MAXLEVEL], *x;
    int i, deleted = 0;

    x = sl->header;
    for (i = sl->level - 1; i >= 0; i--) {
        while (x->level[i].forward
                && (slCmp(x->level[i].forward->score, score) < 0))
            x = x->level[i].forward;
        update[i] = x;
    }
    /* We may have multiple elements with the same score, we
     * need to remove them all */
    x = x->level[0].forward;
    while (x && slCmp(score, x->score) == 0) {
        slNode *next = x->level[0].forward;
        slDeleteNode(sl, x, update);
        slFreeNode(x);
        deleted++;
        x = next;
    }
    return deleted;
}



/*-----------------------------------------------------------------------------
 * implementation of redis commands
 *----------------------------------------------------------------------------*/

void sladdCommand(redisClient *c) {
    robj *key, *slobj, *ele, *score;
    skiplist *sl;
    int added = 0, j, numelements;

    /* validate number of keys */
    if (c->argc % 2) {
        addReply(c, shared.syntaxerr);
        return;
    }

    /* get the key (skiplist) */
    key = c->argv[1];
    slobj = lookupKeyWrite(c->db, key);
    if (slobj == NULL ) {
        slobj = createSkiplistObject();
        dbAdd(c->db, key, slobj);
    } else {
        /* validate that the type and encoding is correct */
        if (slobj->type != REDIS_LIST
                || slobj->encoding != REDIS_ENCODING_SKIPLIST) {
            addReply(c, shared.wrongtypeerr);
        }
    }

    sl = slobj->ptr;
    numelements = (c->argc - 2) / 2;
    for (j = 0; j < numelements; j++) {
        /* try getting the encoded values */
        score = c->argv[2 + j * 2] = tryObjectEncoding(c->argv[2 + j * 2]);
        ele = c->argv[3 + j * 2] = tryObjectEncoding(c->argv[3 + j * 2]);
        /* override the score and value */
        added -= slDelete(sl, score, ele);
        slInsert(sl, score, ele);
        incrRefCount(score);
        incrRefCount(ele);
        added++;
        server.dirty++;
    }

    /* respond with the number of values added to the skiplist */
    addReplyLongLong(c, added);
    if (added) {
        signalModifiedKey(c->db, key);
        notifyKeyspaceEvent(REDIS_NOTIFY_LIST, "sladd", key, c->db->id);
    }
}

void slremCommand(redisClient *c) {
    robj *key, *slobj, *score;
    skiplist *sl;
    int numelements, deleted = 0, keyremoved = 0;
    /* validate number of keys */
    if (!(c->argc % 2)) {
        addReply(c, shared.syntaxerr);
        return;
    }

    key = c->argv[1];
    slobj = lookupKeyWrite(c->db, key);
    if (slobj == NULL || slobj->type != REDIS_LIST
            || slobj->encoding != REDIS_ENCODING_SKIPLIST) {
        addReply(c, shared.emptymultibulk);
        return;
    }

    sl = slobj->ptr;
    /* delete all the elements */
    numelements = c->argc - 2;
    for (int i = 0; i < numelements; i++) {
        score = c->argv[i + 2];
        deleted += slDeleteScore(sl, score);
        if (sl->length == 0) {
            dbDelete(c->db, key);
            keyremoved++;
            break;
        }
    }

    /* report back what happened */
    addReplyLongLong(c, deleted);
    if (deleted) {
        signalModifiedKey(c->db, key);
        notifyKeyspaceEvent(REDIS_NOTIFY_LIST, "slrem", key, c->db->id);
        if (keyremoved) {
            notifyKeyspaceEvent(REDIS_NOTIFY_GENERIC, "del", key, c->db->id);
        }
    }
}

void slallCommand(redisClient *c) {
    slNode *x;
    skiplist *sl;
    robj *slobj, *key, *scoreobj, *eleobj;
    void *replylen = NULL;
    int len = 0;

    key = c->argv[1];
    slobj = lookupKeyRead(c->db, key);
    if (slobj == NULL || slobj->type != REDIS_LIST
            || slobj->encoding != REDIS_ENCODING_SKIPLIST) {
        addReply(c, shared.emptymultibulk);
        return;
    }

    sl = slobj->ptr;
    x = sl->header->level[0].forward;
    replylen = addDeferredMultiBulkLength(c);

    while (x) {
        scoreobj = x->score;
        eleobj = x->obj;
        addReplyBulk(c, scoreobj);
        addReplyBulk(c, eleobj);
        x = x->level[0].forward;
        len++;
    }

    setDeferredMultiBulkLength(c, replylen, 2 * len);
}

void slrangeCommand(redisClient *c) {
    slrangespec range;
    slNode *lowend, *highend;
    skiplist *sl;
    robj *slobj, *key = c->argv[1];
    /* start with 1 here to account for the
     * last element in the range */
    int len = 1;
    slNode *next;
    void *replylen;

    if (slParseRange(c->argv[2], c->argv[3], &range) != REDIS_OK) {
        addReplyError(c, "min or max is not valid");
        return;
    }

    slobj = lookupKeyRead(c->db, key);
    if (slobj == NULL ) {
        slFreeRange(&range);
        addReply(c, shared.emptymultibulk);
        return;
    }
    if (slobj->type != REDIS_LIST
            || slobj->encoding != REDIS_ENCODING_SKIPLIST) {
        slFreeRange(&range);
        addReply(c, shared.wrongtypeerr);
        return;
    }

    sl = slobj->ptr;
    /* search the smallest node in the range
     * if lowend is NULL, there's no point in continuing
     * since the lowest value is out of range */
    lowend = slRangeLowEnd(sl, &range);
    if (lowend == NULL ) {
        slFreeRange(&range);
        addReply(c, shared.emptymultibulk);
        return;
    }

    /* search the largest node in the range */
    highend = slRangeHighEnd(sl, &range);
    if (lowend == NULL || highend == NULL ) {
        slFreeRange(&range);
        addReply(c, shared.emptymultibulk);
        return;
    }

    replylen = addDeferredMultiBulkLength(c);
    next = lowend;

    /* loop over the items found from the beginning */
    while (next && next != highend) {
        addReplyBulk(c, next->score);
        addReplyBulk(c, next->obj);
        next = next->level[0].forward;
        len++;
    }

    /* attach the last two items and send the result out
     * len has been incremented accordingly by starting at 1 */
    addReplyBulk(c, highend->score);
    addReplyBulk(c, highend->obj);
    setDeferredMultiBulkLength(c, replylen, 2 * len);

    slFreeRange(&range);
}

void slsearchCommand(redisClient *c) {
    robj *key = c->argv[1];
    robj *score = c->argv[2];
    robj *slobj;
    slNode *lowend, *x;
    skiplist *sl;
    unsigned long len = 0;
    void *replylen = NULL;

    slobj = lookupKeyRead(c->db, key);
    if (slobj == NULL ) {
        addReply(c, shared.emptymultibulk);
        return;
    }
    if (slobj->type != REDIS_LIST
            || slobj->encoding != REDIS_ENCODING_SKIPLIST) {
        addReply(c, shared.wrongtypeerr);
        return;
    }

    sl = slobj->ptr;
    lowend = slSearchSmallestNode(sl, score);

    if (lowend == NULL ) {
        addReply(c, shared.emptymultibulk);
        return;
    }

    x = lowend;
    replylen = addDeferredMultiBulkLength(c);
    /* we pass on finding the highend
     * since the code most likely will make
     * as many comparisons as we do here */
    while (x && slCmp(x->score, score) == 0) {
        addReplyBulk(c, x->score);
        addReplyBulk(c, x->obj);
        x = x->level[0].forward;
        len++;
    }

    setDeferredMultiBulkLength(c, replylen, 2 * len);
}

void slcardCommand(redisClient *c) {
    robj *slobj;
    robj *key = c->argv[1];
    skiplist *sl;

    slobj = lookupKeyRead(c->db, key);
    if (slobj == NULL || slobj->type != REDIS_LIST
            || slobj->encoding != REDIS_ENCODING_SKIPLIST) {
        addReplyDouble(c, 0);
        return;
    }

    sl = slobj->ptr;
    addReplyDouble(c, sl->length);
}
