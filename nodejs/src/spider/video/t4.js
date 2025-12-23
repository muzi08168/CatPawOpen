import axios from 'axios'

export default function T4Factory(name, address) {
  const api = axios.create({
    baseURL: address
  })

  function init() {
    return {}
  }

  async function home() {
    try {
      const {data} = await api.get('', {
        params: {
          filter: true,
        }
      })
      return data
    } catch (e) {
      console.warn(e)
      return {
        class: []
      }
    }
  }

  async function category(req) {
    try {
      const {data} = await api.get('', {
        params: {
          ac: 'detail',
          t: req.body.id,
          pg: req.body.page || 1
        }
      })
      return data
    } catch (e) {
      console.warn(e)
    }
  }

  async function detail(req) {
    try {
      const {data} = await api.get('', {
        params: {
          ac: 'detail',
          ids: req.body.id,
        }
      })
      return data
    } catch (e) {
      console.warn(e)
    }
  }

  async function play(req) {
    try {
      const {data} = await api.get('', {
        params: {
          play: req.body.id,
          flag: req.body.flag,
        }
      })
      if (!data.url && !data.urls && req.body.id.startsWith('http')) {
        return {
          url: req.body.id
        }
      }
      return data
    } catch (e) {
      console.warn(e)
    }
  }

  async function search(req) {
    try {
      const {data} = await api.get('', {
        params: {
          ac: 'list',
          wd: req.body.wd,
        }
      })
      return data
    } catch (e) {
      console.warn(e)
    }
  }

  return {
    meta: {
      key: name,
      name,
      type: 3,
      t4: true,
    },
    api: async (fastify) => {
      fastify.post('/init', init);
      fastify.post('/home', home);
      fastify.post('/category', category);
      fastify.post('/detail', detail);
      fastify.post('/play', play);
      fastify.post('/search', search);
    },
  }
};