import React, { useEffect, useState } from 'react';
import { Container, Typography, CircularProgress, Alert, Snackbar } from '@mui/material';
import ChannelFilters from '../components/ChannelFilters';
import ParserControl from '../components/ParserControl';
import { getChannels, setFilter, getPauseStatuses, pauseChannel, resumeChannel } from '../api/filters';

const MainPage: React.FC = () => {
  const [channels, setChannels] = useState<any[]>([]);
  const [pauseStatuses, setPauseStatuses] = useState<Record<number, {name: string, paused: boolean}>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [open, setOpen] = useState(false);
  const [pauseLoading, setPauseLoading] = useState<string | null>(null);

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

  const fetchPauseStatuses = async () => {
    try {
      const statuses = await getPauseStatuses();
      setPauseStatuses(statuses);
    } catch {
      setError('Ошибка загрузки статусов паузы');
      setOpen(true);
    }
  };

  useEffect(() => {
    fetchChannels();
    fetchPauseStatuses();
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

  const handlePauseToggle = async (channelName: string, paused: boolean) => {
    setPauseLoading(channelName);
    const oldStatus = pauseStatuses;
    setPauseStatuses(prev => {
      const updated: typeof prev = { ...prev };
      for (const [id, val] of Object.entries(prev)) {
        if (val.name === channelName) {
          updated[id as unknown as number] = { ...val, paused: !paused };
        }
      }
      return updated;
    });
    try {
      if (paused) {
        await resumeChannel(channelName);
        setSuccess('Фильтрация включена');
      } else {
        await pauseChannel(channelName);
        setSuccess('Фильтрация поставлена на паузу');
      }
      setOpen(true);
      fetchPauseStatuses();
    } catch {
      setError('Ошибка управления паузой');
      setPauseStatuses(oldStatus);
      setOpen(true);
    } finally {
      setPauseLoading(null);
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
      
      <ParserControl />
      
      <ChannelFilters channels={channels} onSave={handleSave} pauseStatuses={pauseStatuses} onPauseToggle={handlePauseToggle} pauseLoading={pauseLoading} />
      <Snackbar open={open} autoHideDuration={3000} onClose={handleClose} anchorOrigin={{vertical:'bottom',horizontal:'center'}}>
        {(error || success) ? (
          error ? <Alert severity="error" onClose={handleClose}>{error}</Alert> : <Alert severity="success" onClose={handleClose}>{success}</Alert>
        ) : undefined}
      </Snackbar>
    </Container>
  );
};

export default MainPage; 