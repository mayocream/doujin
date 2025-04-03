import puppeteer from 'puppeteer'
import fs from 'fs'

const browser = await puppeteer.launch()
const page = await browser.newPage()

const baseUrl = 'https://exhentai.org'
const query = new URLSearchParams()

// Cookies
const cookie = process.env.EHENTAI_COOKIE!
cookie.split(';').forEach((c) => {
    const [key, value] = c.split('=')
    browser.setCookie({
        name: key,
        value: value,
        domain: 'exhentai.org',
    })
})

// Search only Doujinshi
query.append('f_cats', '1021')

// Search only for Untranslated
query.append('f_search', '-translated')

// Disable filters
query.append('f_sft', 'on')
query.append('f_sfu', 'on')
query.append('f_sfl', 'on')

const navigatePage = async (url: string) => {
    const u = new URL(url)
    const pageRef = u.searchParams.get('prev')

    await page.goto(url)

    const content = await page.$('.itg.gltm')
    const items = await content?.$$('tr')

    // Skip the first item, which is the header
    items?.shift()

    // Save data per page
    // with current page link and next page link
    const data: any = {
        page: url,
        nextPage: null,
        items: [],
    }

    data.page = url

    const nextPage = await page.$('#dprev')
    const nextPageUrl = await nextPage?.evaluate((el) => el.getAttribute('href'))
    if (nextPageUrl) {
        data.nextPage = nextPageUrl
    }

    for (const item of items!) {
        const title = await item.$eval('.glink', (el) => el.textContent)
        const link = await item.$eval('.glname a', (el) => el.getAttribute('href'))
        const date = await item.$eval('[id^=posted]', (el) => el.textContent)
        const thumbnail = await item.$eval('.glthumb img', (el) => el.getAttribute('src'))

        data.items.push({
            title,
            link,
            date,
            thumbnail,
        })
    }

    // Save data to file
    fs.writeFileSync(`../../data/ehentai/${pageRef}.json`, JSON.stringify(data, null, 2))

    return nextPageUrl
}

const run = async () => {
    const url = new URL(baseUrl)
    query.forEach((value, key) => url.searchParams.append(key, value))

    // Read last page number from directory
    const files = fs.readdirSync('../../data/ehentai')
    const lastPage = files
        .map((file) => parseInt(file.split('.')[0]))
        .sort((a, b) => b - a)[0]

    url.searchParams.append('prev', lastPage.toString())

    let nextPage: any = url.toString()
    while (nextPage) {
        nextPage = await navigatePage(nextPage)
    }
}

while (true) {
    try {
        await run()
        break
    } catch (e) {
        console.error(e)
        continue
    }
}
