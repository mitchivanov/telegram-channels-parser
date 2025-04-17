import axios from 'axios';

const API_URL = '/api/filters';

export const getChannels = async () => {
  const { data } = await axios.get(`${API_URL}/channels`);
  return data;
};

export const setFilter = async (filter: any) => {
  const { data } = await axios.post(`${API_URL}/`, filter);
  return data;
};

