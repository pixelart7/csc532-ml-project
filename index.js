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

const meterRedius = 500

const reportEvery = 50

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
    obj[elm['venueCategory']] = 0
  })
  return obj
}

function storeListNearbyCount (store, venueCategory) {
  const newStoreList = []
  store.forEach((row, i) => { // o(n^2)

    if (i % reportEvery == 0) console.log("At: " + i + ", Time: " + new Date())

    const filledRow = createColumnForVenueCategory(row, venueCategory)
    iLat = parseFloat(row['latitude'])
    iLong = parseFloat(row['longitude'])
    store.forEach((compareRow, j) => {
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
      if (distance <= meterRedius) {
        filledRow[compareRow['venueCategory']] += 1
      }
    })
    newStoreList.push(filledRow)
  })
  return newStoreList
}

(async () => {
  try {
    var venueCategoryList = await read(venueCategoryListPath)
    var storeList = await read(storeListPath)
    var trainList = await read(trainStationListPath)

    // storeList = storeList.slice(0, 2000)

    console.log('Data size: ' + storeList.length )
    const res = storeListNearbyCount(storeList, venueCategoryList)
    const res2 = storeListNearbyCount(storeList, venueCategoryList)

    console.log('done')

    await write(path.join(__dirname, 'output.csv'), csvString.stringify([Object.keys(res[0]), ...res]))
  } catch (e) {
    console.error(e)
    // Deal with the fact the chain failed
  }
})();