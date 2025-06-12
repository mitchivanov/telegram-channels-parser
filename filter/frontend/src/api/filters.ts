import axios from 'axios';

const API_URL = '/api/filters';

export interface Filter {
  id?: number;
  channel_id: number;
  keywords: string[];
  stopwords: string[];
  remove_channel_links: boolean;
  moderation_required: boolean;
  min_cashback_percent?: number;
  max_cashback_percent?: number;
}

export interface FilterCreate {
  channel: string;
  keywords: string[];
  stopwords: string[];
  remove_channel_links: boolean;
  moderation_required: boolean;
  min_cashback_percent?: number;
  max_cashback_percent?: number;
}

export interface ParserStatus {
  parser_paused: boolean;
}

export const getChannels = async () => {
  const { data } = await axios.get(`${API_URL}/channels`);
  return data;
};

export const setFilter = async (filter: any) => {
  const { data } = await axios.post(`${API_URL}/`, filter);
  return data;
};

export const getPauseStatuses = async () => {
  const { data } = await axios.get(`${API_URL}/pause_status`);
  return data;
};

export const pauseChannel = async (channelName: string) => {
  const { data } = await axios.post(`${API_URL}/pause/${channelName}`);
  return data;
};

export const resumeChannel = async (channelName: string) => {
  const { data } = await axios.post(`${API_URL}/resume/${channelName}`);
  return data;
};

// Глобальная пауза парсера
export const getParserStatus = async (): Promise<ParserStatus> => {
  const { data } = await axios.get(`${API_URL}/parser/status`);
  return data;
};

export const pauseParser = async () => {
  const { data } = await axios.post(`${API_URL}/parser/pause`);
  return data;
};

export const resumeParser = async () => {
  const { data } = await axios.post(`${API_URL}/parser/resume`);
  return data;
};

