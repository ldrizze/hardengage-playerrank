const fs = require('fs')
const csv = require('csv')
const _ = require('lodash')

// 1744061915905

/*
    playerName: player.name
    tagLine: player.tagline
    matchIds: playerMatches.data
    matches: []
*/

// PROGRAM
const { program } = require('commander');

program
    .option('--batch-id <char>', 'The id of batch to process')

program.parse()
const options = program.opts()
const batchId = options.batchId

// LOAD FILE
const fd = fs.openSync(`./batches/batch-${batchId}.json`)
const filebuffer = fs.readFileSync(fd)
const data = JSON.parse(filebuffer.toString())
let result = []

// ITRATE THROUGH PLAYERS
for (const puuid of Object.keys(data)) {
    const player = data[puuid]
    const outData = {
        playerName: player.playerName,
        tagLine: player.tagLine,
        kills: 0,
        deaths: 0,
        assists: 0,
        kda: 0,
        gameCount: 0,
        victory: 0,
        defeat: 0,
        teamKills: 0,
        gameTime: 0,
        visionScore: 0,
        totalDamageDealtToChampions: 0
    }

    for (const match of player.matches) {
        if (match.info.endOfGameResult.toLowerCase() !== 'gamecomplete') continue

        // FIND PARTICIPANT ID OF PLAYER
        const participantId = match.metadata.participants.findIndex(value => puuid === value) + 1
        const participantData = match.info.participants.find(value => value.participantId === participantId)

        // FIND PARTICIPANT TEAM
        const participantTeam = match.info.teams.find(v => v.teamId === participantData.teamId)

        if (participantData.win) {
            outData.victory += 1
        } else {
            outData.defeat += 1
        }

        // GET KDA
        outData.assists += participantData.assists
        outData.deaths += participantData.deaths
        outData.kills += participantData.kills
        outData.gameCount += 1
        outData.teamKills += participantTeam.objectives.champion.kills
        outData.gameTime += match.info.gameDuration / (1000 * 60) // em minutos
        outData.visionScore += participantData.visionScore,
        outData.totalDamageDealtToChampions += participantData.totalDamageDealtToChampions
    }

    result.push(outData)
}

if (result.length === 0) return

// ENCONTRAR O VALOR MÍNIMO - o valor mais baixo entre os jogadores
// kda, victory, defeat

const min = {
    kills: _.minBy(result, v => v.kills).kills,
    deaths: _.minBy(result, v => v.deaths).deaths,
    assists: _.minBy(result, v => v.assists).assists,
    kda: _.minBy(result, v => v.kda).kda,
    victory: _.minBy(result, v => v.victory).victory,
    defeat: _.minBy(result, v => v.defeat).defeat,
}

const max = {
    kills: _.maxBy(result, v => v.kills).kills,
    deaths: _.maxBy(result, v => v.deaths).deaths,
    assists: _.maxBy(result, v => v.assists).assists,
    kda: _.maxBy(result, v => v.kda).kda,
    victory: _.maxBy(result, v => v.victory).victory,
    defeat: _.maxBy(result, v => v.defeat).defeat,
}

// (metrica - min) / max - min
// avgs
result = result.map(r => {
    r.kda = (r.kills + r.deaths + r.assists) / r.gameCount    
    r.winRate = r.victory / (r.victory + r.defeat)
    r.killRate = (r.kills + r.assists) / r.teamKills
    r.avgKills = r.kills / r.gameCount
    r.avgDeaths = r.deaths / r.gameCount
    r.avgDamage = r.totalDamageDealtToChampions / (r.gameCount * r.gameTime)
    r.avgVisionScore = r.visionScore / r.gameTime

    r.normalizedKills       = (r.kills - min.kills) / (max.kills - min.kills)
    r.normalizedDeaths      = (r.deaths - min.deaths) / (max.deaths - min.deaths)
    r.normalizedAssists     = (r.assists - min.assists) / (max.assists - min.assists)
    r.normalizedVictory     = (r.victory - min.victory) / (max.victory - min.victory)
    r.normalizedDefeat      = (r.defeat - min.defeat) / (max.defeat - min.defeat)
    return r
})

const resultFile = fs.createWriteStream(`./results/result-${batchId}.csv`)
csv.stringify(
    result,
    {
        header: true
    }
)
.pipe(resultFile)
.on('finish', () => {
    resultFile.close()
})
