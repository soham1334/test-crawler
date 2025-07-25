    // vitest.config.ts
    import { defineConfig } from 'vitest/config';

    export default defineConfig({
      test: {
        globals: true, // Enables Jest-like global APIs (describe, it, expect, vi)
        environment: 'node',
        setupFiles: [], // No global setup needed for this strategy
        include: ['__tests__/**/*.test.ts'], // Adjust if your test path is different
        mockReset: true, // Resets mocks before each test
        restoreMocks: true, // Restores original mock implementations after each test
        clearMocks: true, // Clears mock calls before each test
        fakeTimers: {
          now: new Date('2025-07-24T12:00:00.000Z'), // Set a consistent base time for fake timers
          // toFake: ['Date'], // Only fake Date if needed, Vitest usually handles it well
        },
      },
    });
    