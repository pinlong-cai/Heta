import styles from './HetaGen.module.css';

export default function HetaGenPage() {
  return (
    <div className={styles.page}>
      <div className={styles.inner}>
        <p className={styles.label}>Coming Soon</p>
        <h1 className={styles.title}>HetaGen</h1>
        <p className={styles.desc}>
          Generative pipeline for structured knowledge synthesis.<br />
          Stay tuned.
        </p>
      </div>
    </div>
  );
}
