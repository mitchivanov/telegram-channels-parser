import React, { useEffect, useState } from 'react';
import { Container, Typography, CircularProgress, Alert, Snackbar } from '@mui/material';
import ChannelFilters from '../components/ChannelFilters';
import { getChannels, setFilter } from '../api/filters';

const MainPage: React.FC = () => {
  const [channels, setChannels] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [open, setOpen] = useState(false);

  const fetchChannels = async () => {
    setLoading(true);
    setError('');
    try {
      const data = await getChannels();
      setChannels(data);
    } catch (e) {
      setError('Ошибка загрузки каналов');
      setOpen(true);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchChannels();
  }, []);

  const handleSave = async (filter: any) => {
    try {
      await setFilter(filter);
      setSuccess('Фильтр успешно сохранён');
      setOpen(true);
      fetchChannels();
    } catch {
      setError('Ошибка сохранения фильтра');
      setOpen(true);
    }
  };

  const handleClose = () => {
    setOpen(false);
    setError('');
    setSuccess('');
  };

  if (loading) return <Container sx={{mt:8}}><CircularProgress /></Container>;

  return (
    <Container sx={{mt:4}} maxWidth="md">
      <Typography variant="h4" mb={4}>Каналы и фильтры</Typography>
      <ChannelFilters channels={channels} onSave={handleSave} />
      <Snackbar open={open} autoHideDuration={3000} onClose={handleClose} anchorOrigin={{vertical:'bottom',horizontal:'center'}}>
        {(error || success) ? (
          error ? <Alert severity="error" onClose={handleClose}>{error}</Alert> : <Alert severity="success" onClose={handleClose}>{success}</Alert>
        ) : undefined}
      </Snackbar>
    </Container>
  );
};

export default MainPage; 