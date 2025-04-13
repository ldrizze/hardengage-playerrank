const parquet = require('parquetjs')

const schema = new parquet.ParquetSchema({
    puuid: { type: 'UTF8' },
    playerName: { type: 'UTF8' },
    tagLine: { type: 'UTF8' },
    matchIds: { type: 'UTF8', repeated: true },
    matches: { type: 'JSON', repeated: true },
    lane: { type: 'UTF8', optional: true }
})

const fn = async () => {
    const reader = await parquet.ParquetReader.openFile('./batches/batch-louie.parquet')
    const cursor = reader.getCursor()
    let record = null
    let players = []
    while (record = await cursor.next()) {
        players.push(record)
    }
    
    players = players.map(v => {
        let puuid = 'none'
        for (const m of v.matches) {
            const match = JSON.parse(m)
            const participant = match.info.participants.find(p => p.riotIdGameName.toLowerCase() === v.playerName.toLowerCase())

            if (participant) {
                puuid = participant.puuid
                console.log(v.playerName, puuid)
                break
            }
        }

        v.puuid = puuid
        return v
    })

    const parquetFile = await parquet.ParquetWriter.openFile(schema, `./batches/batch-repaired.parquet`)
    for (const player of players) {
        await parquetFile.appendRow(player)
    }
    await parquetFile.close()
    console.log('Parquet repaired')
}
fn()