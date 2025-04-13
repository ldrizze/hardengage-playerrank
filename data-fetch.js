const axios = require('axios')
const csv = require('csv-parser')
const fs = require('fs')
const delay = ms => new Promise(resolve => setTimeout(resolve, ms))
const term = require( 'terminal-kit' ).terminal
const parquet = require('parquetjs')
const schema = new parquet.ParquetSchema({
    puuid: { type: 'UTF8' },
    playerName: { type: 'UTF8' },
    tagLine: { type: 'UTF8' },
    matchIds: { type: 'UTF8', repeated: true },
    matches: { type: 'JSON', repeated: true },
    lane: { type: 'UTF8', optional: true }
})

// PROGRAM
const { program } = require('commander');

program
    .option('-c, --count <number>', 'Matches to fetch', 100)
    .option('-d, --delay <number>', 'Match fetch between delay (ms)', 1000)
    .option('-p, --pagination <number>', 'Maximum itens per page', 100)
    .option('-e, --retry <times>', 'Retry number of times', 3)
    .option('-w, --retry-wait-time <time>', 'Wait delay to retry (ms)', 30000)
    .option('--batch-id <id>', 'The batch file id', Date.now())
    .argument('<string>')

program.parse()
const options = program.opts()
const batchId = options.batchId
const batchFileName = `batch-${batchId}.parquet`

const riotApi = axios.create({
    baseURL: 'https://americas.api.riotgames.com',
    headers: {
        'X-Riot-Token': program.args[0]
    }
});

// Parse playerlist
const players = []
const matches = {}
fs.createReadStream('playerlist.csv')
    .pipe(csv())
    .on('data', data => players.push(data))
    .on('end', async () => {
        term(`Creating parquet file\n`)
        const parquetFile = await parquet.ParquetWriter.openFile(schema, `./batches/${batchFileName}`)
        term(`Iterating over ${players.length} players\n`)

        // Iterate players
        for (const player of players) {
            term(`Retrieving `)
            term.bold.underline(`${player.name}#${player.tagline}`)
            term('...')

            let playerAccount
            try {
                playerAccount = await riotApi.get(`/riot/account/v1/accounts/by-riot-id/${player.name}/${player.tagline}`)
                term(` account found! Retrieving matches...`)
            } catch (error) {
                if (error instanceof axios.AxiosError) {
                    term(` error ${error.status}, skipping\n\n`)
                } else {
                    term(` account retrieve error, skipping.\n\n`)
                    term(error.message)
                }
                continue
            }
            await delay(+options.delay)

            let playerMatches = []
            for (let start = 0; start < +options.count; start += +options.pagination) {
                const tmpMatches = await riotApi.get(`/lol/match/v5/matches/by-puuid/${playerAccount.data.puuid}/ids`, {
                    params: {
                        count: Math.min(+options.pagination, +options.count - start),
                        type: 'ranked',
                        queue: 420,
                        start
                    }
                })

                if (tmpMatches.data.length === 0) break;
                
                playerMatches = [...playerMatches, ...tmpMatches.data]
                await delay(+options.delay)
            }

            if (playerMatches.length === 0) {
                term(' no match found, skipping...\n')
                continue
            } else {
                term(` found ${playerMatches.length} matches, fetch starting...\n`)
            }

            matches[playerAccount.data.puuid] = {
                puuid: playerAccount.data.puuid,
                playerName: player.name,
                tagLine: player.tagline,
                matchIds: playerMatches,
                matches: [],
                lane: player.line
            }

            let progressBar = term.progressBar({
                title: 'Fetching',
                items: playerMatches.length,
                eta: true,
                percent: true,
                width: 80,
                inline: true
            })

            for (const matchId of playerMatches) {
                progressBar.startItem(matchId)
                let tries = 1;
                while (true) {
                    if (tries > +options.retry) break
                    try {
                        const playerMatch = await riotApi(`/lol/match/v5/matches/${matchId}`)
                        matches[playerAccount.data.puuid].matches.push(JSON.stringify(playerMatch.data))
                        await delay(+options.delay)
                        break
                    } catch (error) {
                        term(error.message)
                        term('\n')
                        term(`Waiting ${options.retryWaitTime}ms before retry`)
                        tries += 1
                        await delay(+options.retryWaitTime)
                    }
                }
                progressBar.itemDone(matchId)
            }

            await parquetFile.appendRow(matches[playerAccount.data.puuid])
            term('\n\n')
        }

        term('Writing parquet file...')
        await parquetFile.close()
        term.bold.green(' done!\n')

    });