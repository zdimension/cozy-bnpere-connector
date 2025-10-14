const { log } = require('cozy-konnector-libs')

const API_ROOT =
  'https://monere-api.epargne-retraite-entreprises.bnpparibas.com/api/v1'

class BNPEREApi {
  constructor(email, token) {
    this.email = email
    this.token = token

    const myHeaders = new Headers()
    myHeaders.append('Authorization', 'Bearer ' + token)
    myHeaders.append('Content-Type', 'application/json')
    myHeaders.append('x-api-version', '2.0.0')
    this.headers = myHeaders
  }

  makeRequestOptions(method, body = null) {
    return {
      method: method,
      headers: this.headers,
      redirect: 'follow',
      body: body
    }
  }

  async fetch(url, method = 'GET', body = null) {
    log('info', `req on ${url}: ${method} ${body}`)
    return await fetch(
      `${API_ROOT}/${url}`,
      this.makeRequestOptions(method, body)
    ).then(response => response.json())
  }

  async getCompanies() {
    return (await this.fetch('companies')).companies
  }

  async getAllOperations(company) {
    let offsetRC = 0
    let offsetES = 0
    const opss = []
    while (true) {
      const ops = await this.fetch(
        `companies/${company}/operations?offsetRC=${offsetRC}&offsetES=${offsetES}&take=50`
      )
      if (ops.operations.length === 0) break
      opss.push(...ops.operations)
      offsetRC = ops.nextOffsetRC
      offsetES = ops.nextOffsetES
    }
    const rawOps = (opss).filter(
      op => op.statusCode === 'Termine'
    )

    await Promise.all(
      rawOps.map(async op => {
        const detail = await this.fetch(
          `companies/${company}/operations/detail/${op.id}`
        )
        op.company = company
        op.card = op.planId
        if (detail.code === 'COMPTABLE_ABONDEMENT') {
          op.amount = detail.abundanceNetAmount
        } else if (detail.code === 'TRANSFERT') {
          op.amount = detail.instructions[0].amountNet
        }
      })
    )
    return rawOps
  }
}

async function getBNPEREData(email, token) {
  const api = new BNPEREApi(email, token)
  const companies = await api.getCompanies()
  return [
    companies.flatMap(c => {
      return c.plans.map(p => {
        p.company = c.companyId
        return p
      })
    }),
    (
      await Promise.all(
        companies.map(async c => {
          return await api.getAllOperations(c.companyId)
        })
      )
    ).flat()
  ]
}

module.exports = {
  getBNPEREData
}
