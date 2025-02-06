import express from 'express';
import bodyParser from 'body-parser';
import { FileWatcherService } from './services/file-watching-service';

const app = express();
const port = 9500;

app.use(bodyParser.json());

// FileProcessingService.getInstance();
// QueueLeakService.getInstance();
const fileWatcherService = FileWatcherService.getInstance();
fileWatcherService.startWatching();

// Start the server
app.listen(port, () => {
  console.log(`Heave Request Server running at http://localhost:${port}`);
});
