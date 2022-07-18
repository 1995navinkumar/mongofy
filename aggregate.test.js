const olympicPlayers = require('./olympicPlayers.js');
const { aggregate } = require("./aggregationPipeline");


test('should filter docs', () => {

    expect(
        aggregate([{
            $match: {
                age: { $lt: 33 }
            }
        }], olympicPlayers)
    )
        .toEqual(
            olympicPlayers.filter(player => {
                return player.age < 33;
            })
        )



    expect(
        aggregate([{
            $match: {
                age: {
                    $not: { $gt: 23, $lt: 40 }
                },
            }
        }], olympicPlayers)
    )
        .toEqual(
            olympicPlayers.filter(player => {
                return !(player.age > 23 && player.age < 40);
            })
        )


    expect(
        aggregate([{
            $match: {
                $and: [{
                    age: { $lt: 23 }
                }, {
                    year: { $gt: 2000 }
                }]
            }
        }], olympicPlayers)
    )
        .toEqual(
            olympicPlayers.filter(player => {
                return player.age < 23 && player.year > 2000;
            })
        )
})


console.log(
    aggregate([
        {
            $group: {
                _id: {
                    year: "$year"
                },
                avgGold: {
                    $avg: "$gold"
                },
                maxGold: {
                    $max: "$gold"
                },
                totalGold: {
                    $sum: "$gold"
                },
                count: {
                    $count: "$"
                }
            }
        },
        {
            $sort: {
                "maxGold": -1
            }
        }
    ], olympicPlayers)
)