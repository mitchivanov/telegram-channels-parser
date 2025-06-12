import React, { useState, useEffect } from 'react';
import { 
  Card, 
  CardContent, 
  Typography, 
  Button, 
  Box, 
  Alert,
  CircularProgress
} from '@mui/material';
import { PlayArrow, Pause } from '@mui/icons-material';
import { getParserStatus, pauseParser, resumeParser, ParserStatus } from '../api/filters';

interface Props {
  onStatusChange?: (status: ParserStatus) => void;
}

const ParserControl: React.FC<Props> = ({ onStatusChange }) => {
  const [parserStatus, setParserStatus] = useState<ParserStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const [actionLoading, setActionLoading] = useState(false);
  const [error, setError] = useState<string>('');

  const fetchParserStatus = async () => {
    setLoading(true);
    setError('');
    try {
      const status = await getParserStatus();
      setParserStatus(status);
      onStatusChange?.(status);
    } catch (e) {
      setError('Ошибка загрузки статуса парсера');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchParserStatus();
    // Обновляем статус каждые 30 секунд
    const interval = setInterval(fetchParserStatus, 30000);
    return () => clearInterval(interval);
  }, []);

  const handleTogglePause = async () => {
    if (!parserStatus) return;
    
    setActionLoading(true);
    setError('');
    
    try {
      if (parserStatus.parser_paused) {
        await resumeParser();
        setParserStatus({ parser_paused: false });
      } else {
        await pauseParser();
        setParserStatus({ parser_paused: true });
      }
      
      // Обновляем статус через секунду для подтверждения
      setTimeout(fetchParserStatus, 1000);
    } catch (e) {
      setError('Ошибка управления парсером');
    } finally {
      setActionLoading(false);
    }
  };

  if (loading && !parserStatus) {
    return (
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Box display="flex" alignItems="center" justifyContent="center">
            <CircularProgress size={24} sx={{ mr: 2 }} />
            <Typography>Загрузка статуса парсера...</Typography>
          </Box>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card sx={{ mb: 3, backgroundColor: parserStatus?.parser_paused ? '#fff3e0' : '#e8f5e8' }}>
      <CardContent>
        <Box display="flex" alignItems="center" justifyContent="space-between">
          <Box>
            <Typography variant="h6" component="h2">
              Управление парсером
            </Typography>
            <Typography 
              variant="body1" 
              color={parserStatus?.parser_paused ? 'warning.main' : 'success.main'}
              sx={{ fontWeight: 'bold' }}
            >
              Статус: {parserStatus?.parser_paused ? 'На паузе' : 'Активен'}
            </Typography>
            {parserStatus?.parser_paused && (
              <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                При возобновлении парсер начнет обрабатывать только новые посты
              </Typography>
            )}
          </Box>
          
          <Box>
            <Button
              variant="contained"
              size="large"
              color={parserStatus?.parser_paused ? 'success' : 'warning'}
              onClick={handleTogglePause}
              disabled={actionLoading}
              startIcon={
                actionLoading ? (
                  <CircularProgress size={20} color="inherit" />
                ) : parserStatus?.parser_paused ? (
                  <PlayArrow />
                ) : (
                  <Pause />
                )
              }
              sx={{ minWidth: 180 }}
            >
              {actionLoading 
                ? 'Обработка...' 
                : parserStatus?.parser_paused 
                  ? 'Возобновить парсинг' 
                  : 'Поставить на паузу'
              }
            </Button>
          </Box>
        </Box>
        
        {error && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {error}
          </Alert>
        )}
      </CardContent>
    </Card>
  );
};

export default ParserControl; 