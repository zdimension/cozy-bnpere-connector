const puppeteer = require('puppeteer')
const { log } = require('cozy-konnector-libs')
const fs = require('fs')
const { setTimeout } = require('timers/promises')

const baseUrl = 'https://monepargne.ere.bnpparibas'
const walletUrl = `${baseUrl}/accueil`

module.exports = {
  getToken: async function (connector, username, password) {
    log('info', 'Get token')
    let dataDir = `./data/${username}`
    // create data dir if it doesn't exist
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir)
    }
    let browser = await puppeteer.launch({
      headless: false,
      userDataDir: dataDir,
      defaultViewport: null,
      args: [
        //'--disable-http2',
      ],
      executablePath: "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
      ignoreDefaultArgs: ['--enable-automation']
    })
    let page = await browser.newPage()

    let access_token = null

    page.on('response', async response => {
      const req = response.request()
      // check if there's an Authorization header
      const authHeader = req.headers()['authorization']
      if (authHeader && authHeader.includes(' '))
        access_token = authHeader.split(' ')[1].trim()
      else if (response.url().endsWith('/token'))
        access_token = (await response.json()).access_token
    })

    // set webdriver to false
    await page.evaluateOnNewDocument(() => {
      Object.defineProperty(navigator, 'webdriver', {
        get: () => false
      })
    })

    await page.goto(walletUrl)

    await setTimeout(500)

    // if page full html is less than 1000 chars, refresh
    let html = await page.content()
    let tries = 0
    while (html.length < 2000) {
      tries++
      if (tries > 5) {
        log('info', 'Failed to load page')
        await browser.close()
        return null
      }
      log('info', 'Refreshing')
      await page.reload()
      // wait for 5 seconds
      await setTimeout(5000)
      html = await page.content()
    }

    // wait for idle
    await setTimeout(1000)

    async function login() {
      // original auth code from @Guekka

      // find element with text "Je me connecte" and click on it
      const logbtn = await page.waitForSelector('::-p-text(Je me connecte)', {
        timeout: 20000
      })
      await Promise.all([page.waitForNavigation(), logbtn.click()])

      await setTimeout(2000)

      await page
        .$('input[placeholder="Adresse e-mail"]')
        .then(el => {
          if (el) el.type(username)
          })
      await setTimeout(500)
      await page.$('input[type="password"]').then(el => el.type(password))

      await setTimeout(500)
      // get elem //div[contains(text(), "Se souvenir")]/parent::div/preceding-sibling::div/div/div
      // click on it
      await page
        .waitForSelector(
          '::-p-xpath(//div[contains(text(), "Se souvenir")]/parent::div/preceding-sibling::div/div/div)'
        )
        .then(el => el.click())

      await setTimeout(2000)

      await page
        .waitForSelector('::-p-text(Se connecter)', { timeout: 20000 })
        .then(el => el.click())

      try {
        await page.waitForFunction(`window.location.href === "${walletUrl}"`, {
          timeout: 40000
        })
      } catch (e) {
        // if url contains oidc/callback, refresh and try again
        if (page.url().includes('oidc/callback')) {
          log('info', 'Refreshing')
          await page.goto(walletUrl)
          await login()
        }
      }
    }

    try {
      await page.waitForFunction(`window.location.href === "${walletUrl}"`, {
        timeout: 5000
      })
    } catch (e) {
      log('info', 'Not logged in, logging in...')
      try {
        const onetrust = '.save-preference-btn-handler'
        const onetrustBtn = await page.waitForSelector(onetrust, {
          timeout: 1000
        })
        log('info', 'Cookies')

        if (onetrustBtn !== null) {
          await Promise.all([page.waitForNetworkIdle(), page.click(onetrust)])
        }
      } catch (e) {
        //
      }
      await login()
    }

    // periodically check if we have the token
    let count = 0
    while (!access_token) {
      await setTimeout(1000)
      count++
      if (count > 30) {
        // refresh
        log('info', 'Refreshing')
        await page.reload()
        await login()
        count = 0
      }
    }

    log('info', `Token value: ${access_token} `)

    await connector.notifySuccessfulLogin()

    await browser.close()
    return access_token
  }
}
