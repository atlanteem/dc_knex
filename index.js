// --- Initial ---

const _ = require('lodash');

var fnLog = null;
var libKnex = null;

function helperSQLCriteria(knex, condition) {
    if (!condition) return knex;

    const criteria = { ...condition };

    if (!!criteria['sortby']) {
        if (!!criteria['order']) {
            if (criteria['order'].toLowerCase() === 'asc') {
                knex = knex.orderBy(criteria['sortby'], 'asc');
            }
            delete criteria['order'];
        } else knex = knex.orderBy(criteria['sortby'], 'desc');

        delete criteria['sortby'];
    }
    if (!!criteria['limit']) {
        knex = knex.limit(criteria['limit']);
        delete criteria['limit'];
    }
    if (!!criteria['offset']) {
        knex = knex.offset(criteria['offset']);
        delete criteria['offset'];
    }

    if (!!criteria['colsets']) {
        for (let cs of criteria['colsets']) {
            if (!!cs['col'] && !!cs['vset']) knex = knex.whereIn(cs['col'], cs['vset']);
        }
        delete criteria['colsets'];
    }
    if (!!criteria['colconditions']) {
        for (let cc of criteria['colconditions']) {
            // !!! cc['val'] may be null !!! in some cases !!!
            if (!!cc['col'] && !!cc['condition']) knex = knex.where(cc['col'], cc['condition'], cc['val']);
        }
        delete criteria['colconditions'];
    }
    if (!!criteria['null']) {
        for (let n of criteria['null']) {
            knex = knex.whereNull(n);
        }
        delete criteria['null'];
    }
    if (!!criteria['notNull']) {
        for (let nn of criteria['notNull']) {
            knex = knex.whereNotNull(nn);
        }
        delete criteria['notNull'];
    }
    if (!!criteria['obj']) {
        knex = knex.where(criteria['obj']);
        delete criteria['obj'];
    } else if (!_.isEmpty(criteria)) {
        knex = knex.where(criteria);
    }

    return knex;
}

// --- Export Class ---

class DataCollection {

    constructor(tableName, viewName, logEnabled = false) {
        if (!libKnex) {
            this.error = 'Knex lib is not ready!';
            return;
        }
        if (!tableName|| !viewName) {
            this.error = 'Reqired parameters not found!';
            return;
        };
        this._tableName = tableName;
        this._viewName = viewName;
        this._logEnabled = logEnabled;
    }
  
    get tableName() { return this._tableName; }
    get viewName()  { return this._viewName; }

    select(criteria) {
        let knex = libKnex(this.viewName);
        knex = helperSQLCriteria(knex, criteria);
        if (this._logEnabled && !!fnLog) fnLog(`db::${this._tableName}.select: `, knex.toString());
        return knex.select();
    }

    insert(recordArray) {
        let knex = libKnex(this._tableName).insert(recordArray).returning('*');
        if (this._logEnabled && !!fnLog) fnLog(`db::${this._tableName}.insert: `, knex.toString());
        return knex;
    }

    delete(criteria) {
        let knex = libKnex(this._tableName);
        knex = helperSQLCriteria(knex, criteria);
        knex.del().returning('*')
        if (this._logEnabled && !!fnLog) fnLog(`db::${this._tableName}.delete: `, knex.toString());
        return knex;
    }
    
    update(criteria, newValue) {
        let knex = libKnex(this._tableName);
        knex = helperSQLCriteria(knex, criteria);
        knex.update(newValue).returning('*')
        if (this._logEnabled && !!fnLog) fnLog(`db::${this._tableName}.update: `, knex.toString());
        return knex;
    }

    clone(ids, fields, alias, flag, createdBy) {
        if (_.isEmpty(ids) || _.isEmpty(fields)) { return null; }
        if (!!alias && !flag) { return null; }

        const targetlist = [ ...fields, ...alias||[] ].map(f => `"${f}"`).join(' , ');
        const fieldlist = fields.map(f => `"${f}"`).join(' , ');
        const aliaslist = !alias ? null : alias.map(a => {
            if (a === 'tree_path') return `text2ltree(concat("${a}"::text, '_${flag}')) as ${a}`;
            else return `concat("${a}"::text, '_${flag}') as ${a}`;
        }).join(' , ');
        const sourcelist= !aliaslist ? fieldlist : `${fieldlist} , ${aliaslist}`;
        const idlist = ids.map(id => `'${id}'`).join(' , ');
        
        const sql = !createdBy ? 
            `INSERT INTO ${this._tableName} (${targetlist}) SELECT ${sourcelist} FROM ${this._tableName} WHERE "id" in (${idlist});`
            :
            `INSERT INTO ${this._tableName} (${targetlist}, "created_by") SELECT ${sourcelist}, '${createdBy}' as created_by FROM ${this._tableName} WHERE "id" in (${idlist});`
            ;
        const knex = libKnex.raw(sql);
        if (this._logEnabled && !!fnLog) fnLog(`db::${this._tableName}.renamePath: `, sql);
        return knex;
    }

    search(keyword, fields) {
        if (_.isEmpty(keyword) || _.isEmpty(fields)) { return null; }

        const fieldlist = fields.join(',');
        const criteria = fields
                        // .map(field => `(to_tsvector('simple', ${field}::text) @@ to_tsquery('simple','%${keyword}%'))`)
                        .map(field => `(${field}::text like '%${keyword}%')`)
                        .join(' OR ')
        const sql = `SELECT ${fieldlist} FROM ${this._viewName} WHERE ${criteria}`;

        const knex = libKnex.raw(sql);
        if (this._logEnabled && !!fnLog) fnLog(`db::${this._tableName}.search: `, sql);
        return knex;
    }

    renamePath(source, target) {
        const sql = `update ${this._tableName} set tree_path=replace(tree_path::text, '${source}', '${target}')::ltree where tree_path <@ '${source}' returning *;`;
        const knex = libKnex.raw(sql);
        if (this._logEnabled && !!fnLog) fnLog(`db::${this._tableName}.renamePath: `, sql);
        return knex;
    }

    removePath(path) {
        const sql = `delete from ${this._tableName} where tree_path <@ '${path}' returning *;`;
        const knex = libKnex.raw(sql);
        if (this._logEnabled && !!fnLog) fnLog(`db::${this._tableName}.removePath: `, sql);
        return knex;
    }
}

function initial(instance, logFunction) {
    libKnex = instance;
    fnLog = logFunction;
}
// --- Exports ---

exports.libKnex         = libKnex;
exports.DataCollection  = DataCollection;
exports.initial         = initial;
