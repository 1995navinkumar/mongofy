function pick(structure, obj) {
    return Object.keys(structure).reduce((a, c) => {
        let expr = structure[c].substring(1);
        a[c] = getBy(obj, expr.split("."));
        return a;
    }, {});
}

function predicatePipe(fns, opName = "every") {
    return doc => fns[opName](fn => fn(doc))
}

function getKey(obj) {
    let entries = Object.entries(obj);
    // sort by keys
    entries.sort((a, b) => a[0] > b[0] ? 1 : -1);
    return entries.flat().join("_")
}


// copied from tanstack react-table

const pathObjCache = new Map()

function getBy(obj, path, def) {
    if (!path) {
        return obj
    }
    const cacheKey = typeof path === 'function' ? path : JSON.stringify(path)

    const pathObj =
        pathObjCache.get(cacheKey) ||
        (() => {
            const pathObj = makePathArray(path)
            pathObjCache.set(cacheKey, pathObj)
            return pathObj
        })()

    let val

    try {
        val = pathObj.reduce((cursor, pathPart) => cursor[pathPart], obj)
    } catch (e) {
        // continue regardless of error
    }
    return typeof val !== 'undefined' ? val : def
}

const reOpenBracket = /\[/g
const reCloseBracket = /\]/g

function makePathArray(obj) {
    return (
        flattenDeep(obj)
            // remove all periods in parts
            .map(d => String(d).replace('.', '_'))
            // join parts using period
            .join('.')
            // replace brackets with periods
            .replace(reOpenBracket, '.')
            .replace(reCloseBracket, '')
            // split it back out on periods
            .split('.')
    )
}

function flattenDeep(arr, newArr = []) {
    if (!Array.isArray(arr)) {
        newArr.push(arr)
    } else {
        for (let i = 0; i < arr.length; i += 1) {
            flattenDeep(arr[i], newArr)
        }
    }
    return newArr
}

module.exports = {
    pick, predicatePipe, getBy, getKey
}