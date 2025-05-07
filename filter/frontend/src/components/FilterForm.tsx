import React, { useState } from 'react';
import { Box, TextField, Button, Checkbox, FormControlLabel, Stack } from '@mui/material';

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
  min_cashback_percent?: number;
  max_cashback_percent?: number;
}

interface Props {
  filter?: Filter;
  channelId: string;
  onSave: (filter: FilterCreate) => void;
  onCancel: () => void;
}

const FilterForm: React.FC<Props> = ({ filter, channelId, onSave, onCancel }) => {
  const [keywords, setKeywords] = useState<string>(filter?.keywords?.join(', ') || '');
  const [stopwords, setStopwords] = useState<string>(filter?.stopwords?.join(', ') || '');
  const [removeLinks, setRemoveLinks] = useState<boolean>(filter?.remove_channel_links ?? true);
  const [moderation, setModeration] = useState<boolean>(filter?.moderation_required ?? false);
  const [minCashback, setMinCashback] = useState<string>(filter?.min_cashback_percent !== undefined ? String(filter.min_cashback_percent) : '');
  const [maxCashback, setMaxCashback] = useState<string>(filter?.max_cashback_percent !== undefined ? String(filter.max_cashback_percent) : '');

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const cashbackVariants = ['кэшбек', 'кешбек', 'cashback', 'кэшбэк', 'кешбэк'];
    let kwArr = keywords.split(',').map(s => s.trim()).filter(Boolean);
    cashbackVariants.forEach(variant => {
      if (!kwArr.some(kw => kw.toLowerCase() === variant)) {
        kwArr.push(variant);
      }
    });
    const filterData: FilterCreate = {
      channel: String(channelId),
      keywords: kwArr,
      stopwords: stopwords.split(',').map(s => s.trim()).filter(Boolean),
      remove_channel_links: removeLinks,
      moderation_required: moderation,
      min_cashback_percent: minCashback ? Number(minCashback) : undefined,
      max_cashback_percent: maxCashback ? Number(maxCashback) : undefined,
    };
    onSave(filterData);
  };

  return (
    <Box component="form" onSubmit={handleSubmit} sx={{ mt: 2 }}>
      <Stack spacing={2}>
        <TextField
          label="Ключевые слова (через запятую)"
          value={keywords}
          onChange={e => setKeywords(e.target.value)}
          fullWidth
        />
        <TextField
          label="Стоп-слова (через запятую)"
          value={stopwords}
          onChange={e => setStopwords(e.target.value)}
          fullWidth
        />
        <FormControlLabel
          control={<Checkbox checked={removeLinks} onChange={e => setRemoveLinks(e.target.checked)} />}
          label="Удалять ссылки на каналы"
        />
        <FormControlLabel
          control={<Checkbox checked={moderation} onChange={e => setModeration(e.target.checked)} />}
          label="Требуется модерация"
        />
        <TextField
          label="Минимальный процент кэшбэка (0-100)"
          type="number"
          value={minCashback}
          onChange={e => setMinCashback(e.target.value.replace(/[^\d]/g, '').slice(0, 3))}
          inputProps={{ min: 0, max: 100 }}
          fullWidth
        />
        <TextField
          label="Максимальный процент кэшбэка (0-100)"
          type="number"
          value={maxCashback}
          onChange={e => setMaxCashback(e.target.value.replace(/[^\d]/g, '').slice(0, 3))}
          inputProps={{ min: 0, max: 100 }}
          fullWidth
        />
        <Stack direction="row" spacing={2}>
          <Button type="submit" variant="contained">Сохранить</Button>
          <Button variant="outlined" onClick={onCancel}>Отмена</Button>
        </Stack>
      </Stack>
    </Box>
  );
};

export default FilterForm; 