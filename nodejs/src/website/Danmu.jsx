import React, { useEffect, useState, useCallback } from 'react';
import { createRoot } from 'react-dom/client';
import { Input, Tabs, List, Spin, message, Empty, Typography, Drawer, Button } from 'antd';
import { SortAscendingOutlined, SortDescendingOutlined } from '@ant-design/icons';
import axios from 'axios';
import './Danmu.less';

const { Search } = Input;
const { Title } = Typography;

// --- 缓存辅助函数 ---
const CACHE_KEY = 'danmu_search_cache';
const CACHE_ORDER_KEY = 'danmu_search_order';
const CACHE_MAX_ITEMS = 3;

/**
 * @function getCache
 * @description 从LocalStorage获取指定关键字的缓存结果。
 * @param {string} keyword - 搜索关键字。
 * @returns {object|null} - 缓存的数据或null。
 */
const getCache = (keyword) => {
  const cacheStr = localStorage.getItem(CACHE_KEY);
  if (!cacheStr) return null;
  const cache = JSON.parse(cacheStr);
  return cache[keyword] || null;
};

/**
 * @function setCache
 * @description 将搜索结果存入LocalStorage，并维护最近3条记录。
 * @param {string} keyword - 搜索关键字。
 * @param {Array} data - 要缓存的搜索结果。
 */
const setCache = (keyword, data) => {
  let order = [];
  try {
    order = JSON.parse(localStorage.getItem(CACHE_ORDER_KEY) || '[]');
  } catch (e) { /* do nothing */ }

  let cache = {};
  try {
    cache = JSON.parse(localStorage.getItem(CACHE_KEY) || '{}');
  } catch (e) { /* do nothing */ }

  order = order.filter(k => k !== keyword);
  order.unshift(keyword);

  cache[keyword] = data;

  if (order.length > CACHE_MAX_ITEMS) {
    const keyToRemove = order.pop();
    delete cache[keyToRemove];
  }

  localStorage.setItem(CACHE_ORDER_KEY, JSON.stringify(order));
  localStorage.setItem(CACHE_KEY, JSON.stringify(cache));
};


const AnimeResults = ({ animes, sourceUrl }) => {
  const [isModalVisible, setIsModalVisible] = useState(false);
  const [selectedAnime, setSelectedAnime] = useState(null);
  const [isReversed, setIsReversed] = useState(false);

  const push = async (episode) => {
    try {
      await axios.post('/website/danmu/push', {
        url: `${sourceUrl}/api/v2/comment/${episode.episodeId}?format=xml`
      });
      message.success(`已推送，请等待弹幕加载完成`);
      setIsModalVisible(false); // Close modal on push
    } catch (e) {
      console.error(e);
      message.error(`推送失败`);
    }
  };

  const showEpisodes = (anime) => {
    setSelectedAnime(anime);
    setIsModalVisible(true);
    setIsReversed(false); // Reset sort order on open
  };

  const reverseEpisodes = () => {
    if (selectedAnime) {
      setSelectedAnime({
        ...selectedAnime,
        episodes: [...selectedAnime.episodes].reverse(),
      });
      setIsReversed(prev => !prev);
    }
  };

  if (!animes || animes.length === 0) {
    return <Empty description="该来源下无结果" />;
  }

  return (
    <>
      <List
        dataSource={animes}
        renderItem={(anime) => (
          <List.Item onClick={() => showEpisodes(anime)}>
            {anime.animeTitle}
          </List.Item>
        )}
      />
      {selectedAnime && (
        <Drawer
          title={selectedAnime.animeTitle}
          placement="bottom"
          onClose={() => setIsModalVisible(false)}
          open={isModalVisible}
          height="80%"
          destroyOnClose
          closable={false}
          bodyStyle={{ padding: 0 }}
          rootClassName='drawer'
          extra={<Button icon={isReversed ? <SortAscendingOutlined /> : <SortDescendingOutlined />} onClick={reverseEpisodes} style={{marginLeft: 16}}/>}
        >
          <List
            dataSource={selectedAnime.episodes}
            renderItem={(episode) => (
              <List.Item onClick={() => push(episode)}>
                {episode.episodeTitle}
              </List.Item>
            )}
          />
        </Drawer>
      )}
    </>
  );
};

export function Danmu() {
  const [sources, setSources] = useState([]);
  const [loading, setLoading] = useState(false);
  const [results, setResults] = useState([]);
  const [searched, setSearched] = useState(false);
  const [inputValue, setInputValue] = useState(
    new URLSearchParams(window.location.search).get('keyword') || ''
  );

  useEffect(() => {
    axios.get('/website/danmu/setting')
      .then(res => {
        if (res.data.code === 0 && res.data.data && res.data.data.urls) {
          setSources(res.data.data.urls.map(url => ({ name: url.name || new URL(url.address).hostname, url: url.address })));
        } else {
          message.error('获取弹幕源失败');
        }
      })
      .catch((e) => {
        console.error(e);
        message.error('获取弹幕源失败')
      });
  }, []);

  const handleSearch = useCallback((value, isInitialLoad = false) => {
    const trimmedValue = value.trim();
    if (!trimmedValue) {
      if (!isInitialLoad) message.warning('请输入搜索关键词');
      return;
    }
    if (sources.length === 0 && !isInitialLoad) {
      message.error('没有可用的弹幕源');
      return;
    }

    setInputValue(trimmedValue);

    if (!isInitialLoad) {
      const url = new URL(window.location);
      url.searchParams.set('keyword', trimmedValue);
      window.history.pushState({}, '', url);
    }

    // 仅在初始加载时检查缓存
    if (isInitialLoad) {
        const cachedResults = getCache(trimmedValue);
        if (cachedResults) {
            setResults(cachedResults);
            setSearched(true);
            setLoading(false);
            message.success('从缓存加载了结果');
            return;
        }
    }

    setLoading(true);
    setSearched(true);
    setResults([]);

    let completedCount = 0;
    const currentSearchResults = [];

    sources.forEach(source => {
      axios.get(`${source.url}/api/v2/search/episodes`, { params: { anime: trimmedValue } })
        .then(response => {
          if (response.data.success && response.data.animes && response.data.animes.length > 0) {
            const newResult = {
              sourceName: source.name,
              sourceUrl: source.url,
              data: response.data,
            };
            currentSearchResults.push(newResult);
            setResults(prev => [...prev, newResult]);
          }
        })
        .catch(error => console.error(`Search from ${source.name} failed:`, error))
        .finally(() => {
          completedCount++;
          if (completedCount === sources.length) {
            setLoading(false);
            // 仅当有结果时才缓存
            if (currentSearchResults.length > 0) {
                setCache(trimmedValue, currentSearchResults);
            }
          }
        });
    });
  }, [sources]);

  useEffect(() => {
    if (sources.length > 0) {
      const params = new URLSearchParams(window.location.search);
      const keywordFromUrl = params.get('keyword');
      if (keywordFromUrl) {
        handleSearch(keywordFromUrl, true);
      }
    }
  }, [sources, handleSearch]);

  const sourceItems = results.map((result, index) => ({
    key: `${result.sourceName}-${index}`,
    label: result.sourceName,
    children: <AnimeResults animes={result.data.animes} sourceUrl={result.sourceUrl} />,
  }));

  return (
    <div className="danmu-container">
      <Title level={2} className={`main-title ${searched ? 'hidden' : ''}`}>弹幕搜索</Title>
      <Search
        placeholder="输入影视名称..."
        enterButton="搜索"
        size="large"
        onSearch={(keyword) => handleSearch(keyword)}
        loading={loading}
        className="search-input"
        value={inputValue}
        onChange={(e) => setInputValue(e.target.value)}
      />
      <div className="result-container">
        {searched && results.length === 0 && loading && (
          <div className="spin-container"><Spin size="large" /></div>
        )}
        {results.length > 0 && <Tabs items={sourceItems} size={'small'} />}
        {searched && results.length === 0 && !loading && (
          <Empty description="未找到相关结果" />
        )}
      </div>
    </div>
  );
}

export function renderDanmu() {
  const root = createRoot(document.getElementById('app'));
  root.render(<Danmu />);
}
