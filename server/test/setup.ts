import { setEffectFallback } from 'src/effect/bridge.js';
import { lift } from 'test/lift.js';

setEffectFallback(lift);
