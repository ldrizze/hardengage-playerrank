const fs = require('fs')
const csv = require('csv')
const term = require( 'terminal-kit' ).terminal
const parquet = require('parquetjs')
const delay = ms => new Promise(resolve => setTimeout(resolve, ms))

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
            teamChampionDamage: 0,
            enemyTotalMinionsKilled: 0,
            enemyGoldEarned: 0,
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

            // FIND PARTICIPANT TEAM
            const participantTeam   = match.info.teams.find(v => v.teamId === participantData.teamId)

            // FIND ENEMY
            const enemyParticipant = match.info.participants.find(
                value => value.teamId !== participantData.teamId &&
                value.teamPosition === participantData.teamPosition
            )

            if (!enemyParticipant) {
                continue // skip if oponent isn't playing
            }
            
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
            outData.gameTime += match.info.gameDuration / 60 // convert to minutes
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
            outData.teamChampionDamage += match.info.participants
                .map(v => v.teamId === participantTeam.teamId ? v.totalDamageDealtToChampions : 0)
                .reduce((p, c) => (p ?? 0) + c)
            outData.enemyTotalMinionsKilled += enemyParticipant?.totalMinionsKilled ?? 0
            outData.enemyGoldEarned += enemyParticipant?.goldEarned ?? 0
            outData.enemyChampExperience += enemyParticipant?.champExperience ?? 0
        }

        result.push(outData)
        progressBar.itemDone(player.playerName)
    }

    term('\n\n')
    if (result.length === 0) return

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
