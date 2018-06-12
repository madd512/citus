/*-------------------------------------------------------------------------
 * foreign_constraint.h
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef FOREIGN_CONSTRAINT_H
#define FOREIGN_CONSTRAINT_H

#include "postgres.h"
#include "postgres_ext.h"
#include "utils/relcache.h"
#include "utils/hsearch.h"
#include "nodes/primnodes.h"

typedef struct FRelGraph
{
	HTAB *nodeMap;
	uint32 *indexToOid;
	int nodeCount;
	bool **transitivityMatrix;
}FRelGraph;

extern void ErrorIfUnsupportedForeignConstraint(Relation relation, char
												distributionMethod,
												Var *distributionColumn, uint32
												colocationId);
extern List * GetTableForeignConstraintCommands(Oid relationId);
extern bool HasForeignKeyToReferenceTable(Oid relationId);
extern bool TableReferenced(Oid relationId);
extern FRelGraph * CreateForeignKeyRelationGraph(void);
List * GetForeignKeyRelation(FRelGraph *frelGraph, Oid relationId, bool isAffecting);

#endif
