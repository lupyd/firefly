import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
  site: 'https://firefly.lupyd.com',
  outDir: '../docs',
  integrations: [
    starlight({
      title: 'Firefly MLS',
      description: 'Documentation for Firefly MLS - End-to-End Encrypted Messaging, Client SDKs, Bots, and Web Embedding.',
      social: {
        github: 'https://github.com/lupyd-foundation/firefly',
      },
      editLink: {
        baseUrl: 'https://github.com/lupyd-foundation/firefly/edit/main/doc-site/',
      },
      sidebar: [
        {
          label: 'Getting Started',
          autogenerate: { directory: 'getting-started' },
        },
        {
          label: 'Client SDK Guide',
          autogenerate: { directory: 'client' },
        },
        {
          label: 'Chatbot Framework',
          autogenerate: { directory: 'bot' },
        },
        {
          label: 'Web Embedding',
          autogenerate: { directory: 'embedding' },
        },
        {
          label: 'API Reference',
          autogenerate: { directory: 'reference' },
        },
      ],
      customCss: [
        // Custom styles can be added here
      ],
    }),
  ],
});
