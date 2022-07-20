import { createInterface } from 'readline';

export const readInput = (query: string): Promise<string> => {
  const readline = createInterface({
    input: process.stdin,
    output: process.stdout
  });
  return new Promise((resolve) => {
    readline.question(query, (answer) => {
      resolve(answer);
      readline.close();
    });
  });
};
