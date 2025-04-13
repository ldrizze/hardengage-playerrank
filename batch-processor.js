const fs = require('fs')
const csv = require('csv')
const term = require( 'terminal-kit' ).terminal
const parquet = require('parquetjs')
const delay = ms => new Promise(resolve => setTimeout(resolve, ms))

// 1744061915905

/*
    playerName: player.name
    tagLine: player.tagline
    matchIds: playerMatches.data
    matches: [],
    lane: player.line
*/

// PROGRAM
const { program } = require('commander');

program
    .option('--batch-id <char>', 'The id of batch to process')

program.parse()
const options = program.opts()
const batchId = options.batchId
const fileName = `batch-${batchId}.parquet`

// LOAD FILE
term(`Loading file `)
term.bold.underline(`${fileName}`)
term('... ')
let data = []
let result = []

const fn = async() => {
    const reader = await parquet.ParquetReader.openFile(`./batches/${fileName}`)
    const cursor = reader.getCursor()
    let record = null
    const spn = await term.spinner()
    while (record = await cursor.next()) {
        record.matches = record.matches.map(v => JSON.parse(v))
        data.push(record)
        await delay(1) // update spinner animation
    }
    await reader.close()
    spn.animate(false)
    term('\n')

    let progressBar = term.progressBar({
        title: 'Processing matches',
        items: data.length,
        eta: true,
        percent: true,
        width: 80
    })

    // ITRATE THROUGH PLAYERS
    for (const player of data) {
        progressBar.startItem(player.playerName)

        const outData = {
            playerName: player.playerName,
            tagLine: player.tagLine,
            line: player.lane,
            gameCount: 0,
            victory: 0,
            defeat: 0,
            kills: 0,
            deaths: 0,
            assists: 0,
            teamKills: 0,
            totalDamageDealtToChampions: 0,
            // dano total da equipe aos campeões -> iterar em todos os jogadores da mesma equipe e somar o dano
            teamChampionDamage: 0,
            // pegar totalMinionsKilled do oponente da mesma lane -> enemyTotalMinionsKilled
            enemyTotalMinionsKilled: 0,
            // pegar goldEarned do oponente
            enemyGoldEarned: 0,
            // pegar champExperience do oponente
            enemyChampExperience: 0,
            gameTime: 0,
            visionScore: 0,
            visionScorePerMinute: 0,
            visionWardsBoughtInGame: 0,
            wardsPlaced: 0,
            wardsKilled: 0,
            goldEarned: 0,
            totalMinionsKilled: 0,
            soloKills: 0,
            firstBloodKill: 0,
            firstBloodAssist: 0,
            champExperience: 0
        }

        for (const match of player.matches) {
            if (match.info.endOfGameResult?.toLowerCase() !== 'gamecomplete') continue

            // FIND PARTICIPANT ID OF PLAYER
            const participantId = match.metadata.participants.findIndex(value => player.puuid === value) + 1
            const participantData = match.info.participants.find(value => value.participantId === participantId)

            if (!participantData) {
                console.log(player.playerName, player.puuid)
                console.log(match.info.participants.map(v => [v.riotIdGameName, v.puuid]))
                process.exit(0)
            }
            
            // FIND PARTICIPANT TEAM
            const participantTeam   = match.info.teams.find(v => v.teamId === participantData.teamId)

            // FIND ENEMY
            const enemyParticipant = match.info.participants.find(
                value => value.teamId !== participantData.teamId &&
                value.teamPosition === participantData.teamPosition
            )
            
            if (participantData.win) {
                outData.victory += 1
            } else {
                outData.defeat += 1
            }

            // GET KDA
            outData.gameCount += 1
            outData.kills += participantData.kills
            outData.deaths += participantData.deaths
            outData.assists += participantData.assists
            outData.teamKills += participantTeam.objectives.champion.kills
            outData.totalDamageDealtToChampions += participantData.totalDamageDealtToChampions
            outData.gameTime += match.info.gameDuration / 60 // em minutos
            outData.visionScore += participantData.visionScore,
            outData.visionScorePerMinute += participantData.challenges.visionScorePerMinute
            outData.visionWardsBoughtInGame += participantData.visionWardsBoughtInGame
            outData.wardsPlaced += participantData.wardsPlaced
            outData.wardsKilled += participantData.wardsKilled
            outData.goldEarned += participantData.goldEarned
            outData.totalMinionsKilled += participantData.totalMinionsKilled
            outData.soloKills += participantData.challenges.soloKills
            outData.firstBloodKill += participantData.firstBloodKill
            outData.firstBloodAssist += participantData.firstBloodAssist
            outData.champExperience += participantData.champExperience
            outData.teamChampionDamage += match.info.participants.map(v => v.teamId === participantTeam.teamId ? v.totalDamageDealtToChampions : 0).reduce((p, c) => (p ?? 0) + c)
            outData.enemyTotalMinionsKilled += enemyParticipant?.totalMinionsKilled ?? 0
            outData.enemyGoldEarned += enemyParticipant?.goldEarned ?? 0
            outData.enemyChampExperience += enemyParticipant?.champExperience ?? 0
        }

        result.push(outData)
        progressBar.itemDone(player.playerName)
    }

    term('\n\n')

    if (result.length === 0) return

    // ENCONTRAR O VALOR MÍNIMO - o valor mais baixo entre os jogadores
    // kda, victory, defeat

    // const min = {
    //     kills: _.minBy(result, v => v.kills).kills,
    //     deaths: _.minBy(result, v => v.deaths).deaths,
    //     assists: _.minBy(result, v => v.assists).assists,
    //     kda: _.minBy(result, v => v.kda).kda,
    //     victory: _.minBy(result, v => v.victory).victory,
    //     defeat: _.minBy(result, v => v.defeat).defeat,
    //     visionScore: _.minBy(result, v => v.visionScore).visionScore,
    // }

    // const max = {
    //     kills: _.maxBy(result, v => v.kills).kills,
    //     deaths: _.maxBy(result, v => v.deaths).deaths,
    //     assists: _.maxBy(result, v => v.assists).assists,
    //     kda: _.maxBy(result, v => v.kda).kda,
    //     victory: _.maxBy(result, v => v.victory).victory,
    //     defeat: _.maxBy(result, v => v.defeat).defeat,
    //     visionScore: _.maxBy(result, v => v.visionScore).visionScore,
    // }

    // (metrica - min) / max - min
    // avgs
    // result = result.map(r => {
    //     r.kda = (r.kills + r.deaths + r.assists) / r.gameCount
    //     r.winRate = r.victory / (r.victory + r.defeat)
    //     r.killRate = (r.kills + r.assists) / r.teamKills
    //     r.avgKills = r.kills / r.gameCount
    //     r.avgDeaths = r.deaths / r.gameCount
    //     r.avgDamage = r.totalDamageDealtToChampions / (r.gameCount * r.gameTime)
    //     r.avgVisionScore = r.visionScore / r.gameCount
    //     r.avgGameVisionScore = r.visionScore / (r.gameCount * r.gameTime)

    //     r.normalizedKills       = (r.kills - min.kills) / (max.kills - min.kills)
    //     r.normalizedDeaths      = (r.deaths - min.deaths) / (max.deaths - min.deaths)
    //     r.normalizedAssists     = (r.assists - min.assists) / (max.assists - min.assists)
    //     r.normalizedVictory     = (r.victory - min.victory) / (max.victory - min.victory)
    //     r.normalizedDefeat      = (r.defeat - min.defeat) / (max.defeat - min.defeat)
    //     r.normalizedVisionScore = (r.visionScore - min.visionScore) / (max.visionScore - min.visionScore)
    //     return r
    // })

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
}
fn()
