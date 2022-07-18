const { predicatePipe } = require("./utils");

const addQueryOperators = ops => {
    Object.assign(queryOperators, ops);
}

const addAggregateOperators = ops => {
    Object.assign(aggregateOperators, ops);
}

const queryOperators = {
    // Comparison Query Operators
    $eq: (field, value) => doc => doc[field] === value,
    $gt: (field, value) => doc => doc[field] > value,
    $gte: (field, value) => doc => doc[field] >= value,
    $in: (field, values) => doc => values.includes(doc[field]),
    $lt: (field, value) => doc => doc[field] < value,
    $lte: (field, value) => doc => doc[field] <= value,
    $ne: (field, value) => doc => doc[field] != value,
    $nin: (field, values) => doc => !(values.includes(doc[field])),

    // Logical Query Operators 
    $not: (field, value) => doc => !predicatePipe(resolveOperators(field, value))(doc),
    $or: (field, values) => doc => {
        let ops = values.map(ops => resolveMatchOperator(ops));
        return predicatePipe(ops, "some")(doc);
    },
    $and: (field, values) => doc => {
        let ops = values.map(ops => resolveMatchOperator(ops));
        return predicatePipe(ops)(doc);
    },
    $nor: (field, values) => !(queryOperators["$or"](field, values)),


    // Element Query Operators
    $exists: (field, value) => doc => value ? field in doc : !(field in doc),

}

const aggregateOperators = {
    $sum: field => {
        let sum = 0;
        return doc => sum += doc[field];
    },
    $count: field => {
        let count = 0;
        return () => ++count;
    },
    $avg: field => {
        let sum = 0;
        let count = 0;
        return doc => {
            sum += doc[field];
            count += 1;
            return sum / count;
        }
    },
    $max: field => {
        let max = -Infinity;
        return doc => {
            max = Math.max(max, doc[field])
            return max;
        }
    },
    $min: field => {
        let max = Infinity;
        return doc => {
            max = Math.min(max, doc[field])
            return max;
        }
    }
}

function resolveOperators(fieldName, ops) {
    return Object
        .entries(ops)
        .map(entry => {
            let [op, value] = entry;
            if (op in queryOperators) {
                return queryOperators[op](fieldName, value)
            } else {
                return () => true
            }
        });
}


function resolveMatchOperator(arg) {
    let fieldNames = Object.keys(arg);

    let [fields, ops] = fieldNames.reduce((a, c) => {
        if (c in queryOperators) {
            a[1].push(c);
        } else {
            a[0].push(c);
        }
        return a;
    }, [[], []]);

    let fieldPredicates = fields.map(fieldName => predicatePipe(resolveOperators(fieldName, arg[fieldName])));
    let opPredicates = ops.map(opName => queryOperators[opName](opName, arg[opName]));
    return predicatePipe([...fieldPredicates, ...opPredicates]);
}

module.exports = {
    aggregateOperators,
    queryOperators,
    resolveMatchOperator,
    resolveOperators
}