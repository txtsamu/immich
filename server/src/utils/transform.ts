import { Matrix, applyToPoint, compose, flipX, flipY, identity, rotate, scale, translate } from 'transformation-matrix';
import type { ImageDimensions } from 'src/types.js';
import { AssetEditAction, AssetEditActionItem } from 'src/dtos/editing.dto.js';

export const getOutputDimensions = (
  edits: AssetEditActionItem[],
  startingDimensions: ImageDimensions,
): ImageDimensions => {
  let { width, height } = startingDimensions;

  const crop = edits.find((edit) => edit.action === AssetEditAction.Crop);
  if (crop) {
    width = crop.parameters.width;
    height = crop.parameters.height;
  }

  for (const edit of edits) {
    if (edit.action !== AssetEditAction.Rotate) {
      continue;
    }

    const angleDegrees = edit.parameters.angle;
    if (angleDegrees === 90 || angleDegrees === 270) {
      [width, height] = [height, width];
    }
  }

  return { width, height };
};

export const createAffineMatrix = (
  edits: AssetEditActionItem[],
  scalingParameters?: {
    pointSpace: ImageDimensions;
    targetSpace: ImageDimensions;
  },
): Matrix => {
  let scalingMatrix: Matrix = identity();

  if (scalingParameters) {
    const { pointSpace, targetSpace } = scalingParameters;
    const scaleX = targetSpace.width / pointSpace.width;
    scalingMatrix = scale(scaleX);
  }

  return compose(
    scalingMatrix,
    ...edits.map((edit) => {
      switch (edit.action) {
        case 'rotate': {
          const angleInRadians = (-edit.parameters.angle * Math.PI) / 180;
          return rotate(angleInRadians);
        }
        case 'mirror': {
          return edit.parameters.axis === 'horizontal' ? flipY() : flipX();
        }
        default: {
          return identity();
        }
      }
    }),
  );
};

export type Point = { x: number; y: number };

type TransformState = {
  points: Point[];
  currentWidth: number;
  currentHeight: number;
};

/**
 * Transforms an array of points through a series of edit operations (crop, rotate, mirror).
 * Points should be in absolute pixel coordinates relative to the starting dimensions.
 */
export const transformPoints = (
  points: Point[],
  edits: AssetEditActionItem[],
  startingDimensions: ImageDimensions,
  { inverse = false } = {},
): TransformState => {
  let currentWidth = startingDimensions.width;
  let currentHeight = startingDimensions.height;
  let transformedPoints = [...points];

  // Handle crop first if not inverting
  if (!inverse) {
    const crop = edits.find((edit) => edit.action === 'crop');
    if (crop) {
      const { x: cropX, y: cropY, width: cropWidth, height: cropHeight } = crop.parameters;
      transformedPoints = transformedPoints.map((p) => ({
        x: p.x - cropX,
        y: p.y - cropY,
      }));
      currentWidth = cropWidth;
      currentHeight = cropHeight;
    }
  }

  // Apply rotate and mirror transforms
  const editSequence = inverse ? edits.toReversed() : edits;
  for (const edit of editSequence) {
    let matrix: Matrix = identity();
    if (edit.action === 'rotate') {
      const angleDegrees = edit.parameters.angle;
      const angleRadians = (angleDegrees * Math.PI) / 180;
      const newWidth = angleDegrees === 90 || angleDegrees === 270 ? currentHeight : currentWidth;
      const newHeight = angleDegrees === 90 || angleDegrees === 270 ? currentWidth : currentHeight;

      matrix = compose(
        translate(newWidth / 2, newHeight / 2),
        rotate(inverse ? -angleRadians : angleRadians),
        translate(-currentWidth / 2, -currentHeight / 2),
      );

      currentWidth = newWidth;
      currentHeight = newHeight;
    } else if (edit.action === 'mirror') {
      matrix = compose(
        translate(currentWidth / 2, currentHeight / 2),
        edit.parameters.axis === 'horizontal' ? flipY() : flipX(),
        translate(-currentWidth / 2, -currentHeight / 2),
      );
    } else {
      // Skip non-affine transformations
      continue;
    }

    transformedPoints = transformedPoints.map((p) => applyToPoint(matrix, p));
  }

  // Handle crop last if inverting
  if (inverse) {
    const crop = edits.find((edit) => edit.action === 'crop');
    if (crop) {
      const { x: cropX, y: cropY } = crop.parameters;
      transformedPoints = transformedPoints.map((p) => ({
        x: p.x + cropX,
        y: p.y + cropY,
      }));
    }
  }

  return {
    points: transformedPoints,
    currentWidth,
    currentHeight,
  };
};
