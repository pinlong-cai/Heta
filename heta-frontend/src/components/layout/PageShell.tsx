import styles from './PageShell.module.css';

interface PageShellProps {
  title: string;
  actions?: React.ReactNode;
  children: React.ReactNode;
}

export default function PageShell({ title, actions, children }: PageShellProps) {
  return (
    <div className={styles.shell}>
      <header className={styles.header}>
        <h1 className={styles.title}>{title}</h1>
        {actions && <div className={styles.actions}>{actions}</div>}
      </header>
      <main className={styles.content}>{children}</main>
    </div>
  );
}
