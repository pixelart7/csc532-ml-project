require('dotenv-extended').load()

const fs = require('fs')
const path = require('path')
const _ = require('lodash')
const csv = require('csv-parser')
const csvString = require('csv-string')
const geolib = require('geolib')

console.log(process.env)

const venueCategoryListPath = process.env.PATH_VENUE_CATEGORY
const storeListPath = process.env.PATH_STORE_LIST
const trainStationListPath = process.env.PATH_TRAIN_STATION_LIST

const meterRedius1 = 500
const meterRedius2 = 1000
const meterRedius3 = 2000

const reportEvery = 50
const saveEvery = 500

function write (path, text) {
  return new Promise ((resolve, reject) => {
    fs.writeFile(path, text, function (err) {
      if (err) {
        reject(console.log(err))
      }
      resolve()
    }); 
  })
}

function read (path) {
  return new Promise ((resolve, reject) => {
    var arr = []
    fs.createReadStream(path)
      .pipe(csv())
      .on('data', (data) => arr.push(data))
      .on('end', () => {
        console.log('Finished: ' + path)
        resolve(arr)
      });
  })
}

function createColumnForVenueCategory (obj, venueCategory) {
  venueCategory.forEach((elm) => {
    obj[`${meterRedius1}-${elm['venueCategory']}`] = 0
    obj[`${meterRedius2}-${elm['venueCategory']}`] = 0
    obj[`${meterRedius3}-${elm['venueCategory']}`] = 0
  })
  return obj
}

async function asyncForEach(array, callback) {
  for (let index = 0; index < array.length; index++) {
    await callback(array[index], index, array);
  }
}

async function storeListNearbyCount (store, venueCategory) {
  let newStoreList = []
  await asyncForEach(store, async (row, i) => { // o(n^2)

    if (i % reportEvery == 0) console.log("At: " + i + ", Time: " + new Date())
    if (i !== 0 && i % saveEvery == 0) {
      console.log(`> Saving Chunk ${i - saveEvery}-${i - 1}...`)
      await write(path.join(__dirname, `output-${i - saveEvery}-${i - 1}.csv`), csvString.stringify([Object.keys(newStoreList[0]), ...newStoreList]))
      newStoreList = []
    }

    const filledRow = createColumnForVenueCategory(row, venueCategory)
    iLat = parseFloat(row['latitude'])
    iLong = parseFloat(row['longitude'])
    await asyncForEach(store, async (compareRow, j) => {
      if ( i == j) return;
      jLat = parseFloat(compareRow['latitude'])
      jLong = parseFloat(compareRow['longitude'])
      // const distance = measure(iLat, iLong, jLat, jLong)
      const distance = geolib.getDistance({
        latitude: iLat,
        longitude: iLong
      }, {
        latitude: jLat,
        longitude: jLong
      })
      if (distance <= meterRedius1) {
        filledRow[`${meterRedius1}-${compareRow['venueCategory']}`] += 1
      }
      if (distance <= meterRedius2) {
        filledRow[`${meterRedius2}-${compareRow['venueCategory']}`] += 1
      }
      if (distance <= meterRedius3) {
        filledRow[`${meterRedius3}-${compareRow['venueCategory']}`] += 1
      }
    })
    newStoreList.push(filledRow)

    if (i === store.length - 1) {
      await write(path.join(__dirname, `output-last.csv`), csvString.stringify([Object.keys(newStoreList[0]), ...newStoreList]))
    }

  })
  
}

(async () => {
  try {
    var venueCategoryList = await read(venueCategoryListPath)
    var storeList = await read(storeListPath)
    var trainList = await read(trainStationListPath)

    // storeList = storeList.slice(0, 100)

    console.log('Data size: ' + storeList.length )
    const res = await storeListNearbyCount(storeList, venueCategoryList)
    // const res2 = storeListNearbyCount(storeList, venueCategoryList)

    console.log('done')

    // await write(path.join(__dirname, 'output-last-chunk.csv'), csvString.stringify([Object.keys(res[0]), ...res]))
  } catch (e) {
    console.error(e)
    // Deal with the fact the chain failed
  }
})();