package pigox

import (
	"fmt"
	"strings"
)

func isInformational(query string) bool {
	return strings.Contains(query, "FROM pg_catalog.")
}

func rewriteInformationalQuery(query string) (string, error) {
	if strings.Contains(query, `WHERE c.oid = i.inhrelid`) {
		return `select 1 limit 0;`, nil
	} else if strings.Contains(query, `WHERE c.oid = i.inhparent`) {
		return `select 1 limit 0;`, nil
	} else if strings.Contains(query, `WHERE p.puballtables AND`) {
		return `select 1 limit 0;`, nil
	} else if strings.Contains(query, `WHERE stxrelid =`) {
		return `select 1 limit 0`, nil
	} else if strings.Contains(query, `WHERE pol.polrelid =`) {
		return `select 1 limit 0`, nil
	} else if strings.Contains(query, `WHERE a.attrelid =`) {
		groups := sqlStringRe.FindStringSubmatch(query)
		if len(groups) < 2 {
			return "", fmt.Errorf(`"\d <table>" is not supported`)
		}
		tableName := groups[1]
		return fmt.Sprintf(`select column_name as attname, data_type as format_type, '' as pg_get_expr, false as attnotnull, '' attcollation, '' as attidentity, '' as attgenerated from information_schema.columns where table_name='%s';`, tableName), nil
	} else if strings.Contains(query, `WHERE c.oid = `) {
		return `select 0 as relchecks, 'r' as relkind, false as relhasindex, false as relhasrules, false as relhastriggers, false as relrowsecurity, false as relforcerowsecurity, false as relhasoids, false as relispartition, '', 0 as reltablespace, '' as reloftype, 'p' as relpersistence, 'd' as relreplident, 'heap' as amname;`, nil
	} else if strings.Contains(query, `OPERATOR(pg_catalog.~)`) {
		groups := sqlStringRe.FindStringSubmatch(query)
		if len(groups) < 2 {
			return "", fmt.Errorf(`"\d <table>" is not supported`)
		}
		tableName := groups[1][2 : len(groups[1])-2]
		return fmt.Sprintf(`select '%s' as oid, 'iox' as nspname, '%s' as relname`, tableName, tableName), nil
	}
	if strings.Contains(query, `AND n.nspname <> 'pg_catalog'`) {
		return `select table_schema as "Schema", table_name as "Name", table_type as "Type", '' as "Owner" from information_schema.tables where table_schema not in ('system', 'information_schema');`, nil
	}
	return `select table_schema as "Schema", table_name as "Name", table_type as "Type", '' as "Owner" from information_schema.tables;`, nil
}
