// Morphing glyph spinner — original Claude Code character sequence.
// Label and color are randomised on each mount for variety.

import { useEffect, useRef, useState } from 'react';
import styles from './Spinner.module.css';

const CHARS = ['⣾', '⣽', '⣻', '⢿', '⡿', '⣟', '⣯', '⣷'];
const INTERVAL_MS = 80;

const LABELS = [
  'Thinking...',
  'Reasoning...',
  'Pondering...',
  'Reflecting...',
  'Analyzing...',
  'Considering...',
];

// Warm, muted tones that feel at home in the beige-based palette
const COLORS = [
  '#C8855A', // terracotta
  '#D4A050', // amber
  '#C87060', // coral
  '#B89168', // caramel
  '#D48840', // golden orange
  '#A08878', // warm taupe
];

function pick<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

interface Props {
  label?: string;
}

export default function Spinner({ label }: Props) {
  const [frame, setFrame] = useState(0);
  // Randomise label and color once per mount (each thinking session)
  const [text]  = useState(() => label ?? pick(LABELS));
  const [color] = useState(() => pick(COLORS));
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    timerRef.current = setInterval(() => {
      setFrame((f) => (f + 1) % CHARS.length);
    }, INTERVAL_MS);
    return () => { if (timerRef.current) clearInterval(timerRef.current); };
  }, []);

  return (
    <div className={styles.root}>
      <span key={frame} className={styles.glyph} style={{ color }} aria-hidden="true">
        {CHARS[frame]}
      </span>
      <span className={styles.label} style={{ color }}>{text}</span>
    </div>
  );
}
