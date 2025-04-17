import React, { useState } from 'react';
import { Box, TextField, Button, Checkbox, FormControlLabel, Stack } from '@mui/material';

interface Filter {
  id?: number;
  channel_id: number;
  keywords: string[];
  stopwords: string[];
  remove_channel_links: boolean;
  moderation_required: boolean;
}

interface FilterCreate {
  channel: string;
  keywords: string[];
  stopwords: string[];
  remove_channel_links: boolean;
  moderation_required: boolean;
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

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const filterData: FilterCreate = {
      channel: String(channelId),
      keywords: keywords.split(',').map(s => s.trim()).filter(Boolean),
      stopwords: stopwords.split(',').map(s => s.trim()).filter(Boolean),
      remove_channel_links: removeLinks,
      moderation_required: moderation,
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
        <Stack direction="row" spacing={2}>
          <Button type="submit" variant="contained">Сохранить</Button>
          <Button variant="outlined" onClick={onCancel}>Отмена</Button>
        </Stack>
      </Stack>
    </Box>
  );
};

export default FilterForm; 