const { pick, getBy, getKey } = require('./utils.js');
const { aggregateOperators, resolveMatchOperator } = require("./operators.js");

const stageProcessor = {
    $match: (arg, data) => {
        let predicate = resolveMatchOperator(arg);
        return data.filter(predicate);
    },
    $group: (arg, data) => {
        let { _id, ...aggregates } = arg;

        let aggMap = {};
        let grouped = data.reduce((a, c) => {
            let newDoc = {
                _id: pick(_id, c),
            }
            let key = getKey(newDoc._id);

            if (!(key in aggMap)) {
                aggMap[key] = getAggregates(aggregates);
            }

            let aggResults = Object.keys(aggMap[key]).reduce((a, fName) => {
                a[fName] = aggMap[key][fName](c);
                return a;
            }, {});

            a[key] = Object.assign(newDoc, aggResults);
            return a;
        }, {});
        return Object.values(grouped);
    },
    $sort: (arg, data) => {
        let sortByEntries = Object.entries(arg);

        let sortFns = sortByEntries.map(entry => {
            return (a, b) => {
                let [key, dir] = entry;

                let [aVal, bVal] = [getBy(a, key.split(".")), getBy(b, key.split("."))]

                if (aVal > bVal) {
                    return dir === 1 ? 1 : -1;
                } else if (aVal < bVal) {
                    return dir === 1 ? -1 : 1;
                } else {
                    return 0
                }
            }
        })

        let sortFn = (docA, docB) => {
            for (let i = 0; i < sortFns.length; i++) {
                const fn = sortFns[i];
                const sortResult = fn(docA, docB);
                if (sortResult === 0) {
                    continue;
                } else {
                    return sortResult;
                }
            }
            return 0;
        };

        return data.sort(sortFn);
    }
}

function getAggregates(aggregates) {
    let aggregateFns = Object.keys(aggregates).reduce((a, c) => {
        let [aggFnName, field] = Object.entries(aggregates[c])[0];
        a[c] = aggregateOperators[aggFnName](field.substring(1));
        return a;
    }, {});
    return aggregateFns;
}



function aggregate(pipeline, data = []) {
    return pipeline.reduce((a, stage) => {
        const stageName = Object.keys(stage);
        const arg = stage[stageName];
        return stageProcessor[stageName](arg, a);
    }, data);


    data.reduce(pipelineReducer, []);
}

module.exports = {
    aggregate
}