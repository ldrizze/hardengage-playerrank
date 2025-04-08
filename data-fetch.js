const axios = require('axios')
const csv = require('csv-parser')
const fs = require('fs')
const delay = ms => new Promise(resolve => setTimeout(resolve, ms))
const term = require( 'terminal-kit' ).terminal;

// PROGRAM
const { program } = require('commander');

program
    .option('-c, --count <number>', 'Matches to fetch', 100)
    .option('-r, --rank <rank>', 'Player rank', 'challenger')
    .option('-d, --delay <number>', 'Match fetch between delay (ms)', 1000)
    .option('-p, --pagination <number>', 'Maximum itens per page', 100)
    .option('-e, --retry <times>', 'Retry number of times', 3)
    .option('-w, --retry-wait-time <time>', 'Wait delay to retry (ms)', 30000)
    .option('--batch-id <id>', 'The batch file id', Date.now())
    .option('--save', 'Save batch procress', true)
    .argument('<string>')

program.parse()
const options = program.opts()
const batchId = options.batchId

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
        term(`Iterating over ${players.length} players\n`)

        // Iterate players
        for (const player of players) {
            term(`Retrieving `)
            term.bold.underline(`${player.name}#${player.tagline}`)
            term('...')
            const playerAccount = await riotApi.get(`/riot/account/v1/accounts/by-riot-id/${player.name}/${player.tagline}`)
            await delay(+options.delay)

            term(` account found! Retrieving matches... `)
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
                term('No match found, skipping...\n')
                continue
            } else {
                term(`Found ${playerMatches.length} matches, starting.\n`)
            }

            matches[playerAccount.data.puuid] = {
                playerName: player.name,
                tagLine: player.tagline,
                matchIds: playerMatches,
                matches: []
            }
            
            let progressBar = term.progressBar({
                title: 'Fetching matches',
                items: playerMatches.length,
                eta: true,
                percent: true,
                width: 80
            })

            for (const matchId of playerMatches) {
                progressBar.startItem(matchId)
                let tries = 1;
                while (true) {
                    if (tries > +options.retry) break
                    try {
                        const playerMatch = await riotApi(`/lol/match/v5/matches/${matchId}`)
                        matches[playerAccount.data.puuid].matches.push(playerMatch.data)
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
            term('\r\n\r\n')
        }

        if (options.save) {
            const batchFileName = `batch-${batchId}.json`
            fs.writeFileSync(`./batches/${batchFileName}`, JSON.stringify(matches))
            term(`Saving batch ${batchFileName}\n`)
        }

    });