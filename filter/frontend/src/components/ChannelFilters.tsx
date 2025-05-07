import React, { useState} from 'react';
import { Box, Typography, Card, CardContent, Button, Stack, Chip } from '@mui/material';
import FilterForm from './FilterForm';
import channelNames from '../channelNames';

interface Filter {
  id?: number;
  channel_id: number;
  keywords: string[];
  stopwords: string[];
  remove_channel_links: boolean;
  moderation_required: boolean;
  min_cashback_percent?: number;
  max_cashback_percent?: number;
}

interface FilterCreate {
  channel: string;
  keywords: string[];
  stopwords: string[];
  remove_channel_links: boolean;
  moderation_required: boolean;
}

interface Channel {
  id: number;
  name: string;
  filters: Filter[];
}

interface Props {
  channels: Channel[];
  onSave: (filter: FilterCreate) => void;
  pauseStatuses: Record<number, {name: string, paused: boolean}>;
  onPauseToggle: (channelName: string, paused: boolean) => void;
  pauseLoading: string | null;
}

const ChannelFilters: React.FC<Props> = ({ channels, onSave, pauseStatuses, onPauseToggle, pauseLoading }) => {
  const [editing, setEditing] = useState<{[key: number]: boolean}>({});

  const safeChannels = Array.isArray(channels) ? channels : [];

  return (
    <Stack spacing={3}>
      {safeChannels.map(channel => {
        const filter = channel.filters[0];
        const pauseStatus = pauseStatuses[channel.id];
        const isPaused = pauseStatus ? pauseStatus.paused : true;
        return (
          <Card key={channel.id}>
            <CardContent>
              <Typography variant="h6">Канал: {channelNames[channel.name] || `Канал ${channel.name}`}</Typography>
              <Box mb={1}>
                <Typography variant="subtitle2" color={isPaused ? 'error' : 'success.main'}>
                  Статус фильтрации: {isPaused ? 'На паузе' : 'Активен'}
                </Typography>
                <Button
                  sx={{mt:1, mb:1}}
                  variant={isPaused ? 'contained' : 'outlined'}
                  color={isPaused ? 'success' : 'warning'}
                  onClick={() => onPauseToggle(channel.name, isPaused)}
                  disabled={pauseLoading === channel.name}
                >
                  {pauseLoading === channel.name ? '...' : (isPaused ? 'Включить фильтрацию' : 'Поставить на паузу')}
                </Button>
              </Box>
              {filter ? (
                <>
                  <Box mb={1}>
                    <Typography variant="subtitle2">Ключевые слова:</Typography>
                    {filter.keywords.length ? filter.keywords.map((kw, i) => <Chip key={i} label={kw} sx={{mr:1, mb:1}} />) : <em>нет</em>}
                  </Box>
                  <Box mb={1}>
                    <Typography variant="subtitle2">Стоп-слова:</Typography>
                    {filter.stopwords.length ? filter.stopwords.map((sw, i) => <Chip key={i} label={sw} sx={{mr:1, mb:1}} />) : <em>нет</em>}
                  </Box>
                  <Typography>Удалять ссылки на каналы: {filter.remove_channel_links ? 'Да' : 'Нет'}</Typography>
                  <Typography>Требуется модерация: {filter.moderation_required ? 'Да' : 'Нет'}</Typography>
                  {typeof filter.min_cashback_percent !== 'undefined' || typeof filter.max_cashback_percent !== 'undefined' ? (
                    <Typography>
                      Диапазон кэшбэка: {typeof filter.min_cashback_percent !== 'undefined' ? filter.min_cashback_percent : 0}
                      % – {typeof filter.max_cashback_percent !== 'undefined' ? filter.max_cashback_percent : 100}%
                      {filter.max_cashback_percent === 100 ? ' (включая бесплатно)' : ''}
                    </Typography>
                  ) : null}
                  <Button sx={{mt:2}} variant="outlined" onClick={() => setEditing(e => ({...e, [channel.id]: true}))}>Редактировать фильтр</Button>
                  {editing[channel.id] && <FilterForm filter={filter} channelId={channel.name} onSave={f => {onSave(f); setEditing(e => ({...e, [channel.id]: false}));}} onCancel={() => setEditing(e => ({...e, [channel.id]: false}))} />}
                </>
              ) : (
                <>
                  <Typography color="text.secondary">Фильтр не задан</Typography>
                  <Button sx={{mt:2}} variant="contained" onClick={() => setEditing(e => ({...e, [channel.id]: true}))}>Добавить фильтр</Button>
                  {editing[channel.id] && <FilterForm channelId={channel.name} onSave={f => {onSave(f); setEditing(e => ({...e, [channel.id]: false}));}} onCancel={() => setEditing(e => ({...e, [channel.id]: false}))} />}
                </>
              )}
            </CardContent>
          </Card>
        );
      })}
    </Stack>
  );
};

export default ChannelFilters; 