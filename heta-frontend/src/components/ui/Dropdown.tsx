import { useEffect, useRef, useState } from 'react';
import styles from './Dropdown.module.css';

interface Option {
  value: string;
  label: string;
}

interface Props {
  options: Option[];
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  disabled?: boolean;
}

export default function Dropdown({ options, value, onChange, placeholder = 'Select…', disabled }: Props) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  const selected = options.find((o) => o.value === value);

  // Close on outside click or Escape
  useEffect(() => {
    if (!open) return;
    function handleClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    }
    function handleKey(e: KeyboardEvent) {
      if (e.key === 'Escape') setOpen(false);
    }
    document.addEventListener('mousedown', handleClick);
    document.addEventListener('keydown', handleKey);
    return () => {
      document.removeEventListener('mousedown', handleClick);
      document.removeEventListener('keydown', handleKey);
    };
  }, [open]);

  function handleSelect(val: string) {
    onChange(val);
    setOpen(false);
  }

  return (
    <div ref={ref} className={[styles.root, disabled ? styles.disabled : ''].join(' ')}>
      <button
        type="button"
        className={[styles.trigger, open ? styles.triggerOpen : ''].join(' ')}
        onClick={() => !disabled && setOpen((o) => !o)}
        disabled={disabled}
      >
        <span className={selected ? styles.triggerText : styles.triggerPlaceholder}>
          {selected ? selected.label : placeholder}
        </span>
        <span className={[styles.arrow, open ? styles.arrowUp : ''].join(' ')}>▾</span>
      </button>

      {open && (
        <div className={styles.menu}>
          {options.length === 0 && (
            <div className={styles.empty}>No options</div>
          )}
          {options.map((opt) => (
            <button
              key={opt.value}
              type="button"
              className={[styles.item, opt.value === value ? styles.itemActive : ''].join(' ')}
              onClick={() => handleSelect(opt.value)}
            >
              {opt.label}
              {opt.value === value && <span className={styles.check}>✓</span>}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
